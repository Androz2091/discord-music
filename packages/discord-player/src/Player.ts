import { Client, GuildResolvable, Snowflake, VoiceState, IntentsBitField, User, ChannelType } from 'discord.js';
import { TypedEmitter as EventEmitter } from 'tiny-typed-emitter';
import { Queue } from './Structures/Queue';
import { VoiceUtils } from './VoiceInterface/VoiceUtils';
import { PlayerEvents, PlayerOptions, QueryType, SearchOptions, PlayerInitOptions, PlaylistInitData, SearchQueryType } from './types/types';
import Track from './Structures/Track';
import { QueryResolver } from './utils/QueryResolver';
import { Util } from './utils/Util';
import { PlayerError, ErrorStatusCode } from './Structures/PlayerError';
import { Playlist } from './Structures/Playlist';
import { generateDependencyReport } from '@discordjs/voice';
import { ExtractorExecutionContext } from './extractors/ExtractorExecutionContext';
import { Collection } from '@discord-player/utils';
import { BaseExtractor } from './extractors/BaseExtractor';
import { SearchResult } from './Structures/SearchResult';

class Player extends EventEmitter<PlayerEvents> {
    public readonly client: Client;
    public readonly options: PlayerInitOptions = {
        autoRegisterExtractor: true,
        ytdlOptions: {
            highWaterMark: 1 << 25
        },
        connectionTimeout: 20000,
        smoothVolume: true,
        lagMonitor: 30000
    };
    public readonly queues = new Collection<Snowflake, Queue>();
    public readonly voiceUtils = new VoiceUtils();
    public requiredEvents = ['error', 'connectionError'] as string[];
    public extractors = new ExtractorExecutionContext(this);
    #lastLatency = -1;

    /**
     * Creates new Discord Player
     * @param {Client} client The Discord Client
     * @param {PlayerInitOptions} [options] The player init options
     */
    constructor(client: Client, options: PlayerInitOptions = {}) {
        super();

        /**
         * The discord.js client
         * @type {Client}
         */
        this.client = client;

        if (this.client?.options?.intents && !new IntentsBitField(this.client?.options?.intents).has(IntentsBitField.Flags.GuildVoiceStates)) {
            throw new PlayerError('client is missing "GuildVoiceStates" intent');
        }

        /**
         * The extractors collection
         * @type {ExtractorModel}
         */
        this.options = Object.assign(this.options, options);

        this.client.on('voiceStateUpdate', this._handleVoiceState.bind(this));

        if (this.options?.autoRegisterExtractor) {
            let nv: any; // eslint-disable-line @typescript-eslint/no-explicit-any

            if ((nv = Util.require('@discord-player/extractor'))) {
                ['YouTubeExtractor', 'SoundCloudExtractor', 'ReverbnationExtractor', 'VimeoExtractor', 'AttachmentExtractor'].forEach((ext) => void this.extractors.register(nv[ext]));
            }
        }

        if (typeof this.options.lagMonitor === 'number' && this.options.lagMonitor > 0) {
            setInterval(() => {
                const start = performance.now();
                setTimeout(() => {
                    this.#lastLatency = performance.now() - start;
                }, 0).unref();
            }, this.options.lagMonitor).unref();
        }
    }

    /**
     * Event loop lag
     * @type {number}
     */
    get eventLoopLag() {
        return this.#lastLatency;
    }

    /**
     * Generates statistics
     */
    generateStatistics() {
        return this.queues.map((m) => m.generateStatistics());
    }

    /**
     * Handles voice state update
     * @param {VoiceState} oldState The old voice state
     * @param {VoiceState} newState The new voice state
     * @returns {void}
     * @private
     */
    private _handleVoiceState(oldState: VoiceState, newState: VoiceState): void {
        const queue = this.getQueue(oldState.guild.id);
        if (!queue || !queue.connection) return;

        this.emit('voiceStateUpdate', queue, oldState, newState);

        if (oldState.channelId && !newState.channelId && newState.member!.id === newState.guild.members.me!.id) {
            try {
                queue.destroy();
            } catch {
                /* noop */
            }
            return void this.emit('botDisconnect', queue);
        }

        if (!oldState.channelId && newState.channelId && newState.member!.id === newState.guild.members.me!.id) {
            if (newState.serverMute != null && oldState.serverMute !== newState.serverMute) {
                queue.setPaused(newState.serverMute);
            } else if (newState.channel?.type === ChannelType.GuildStageVoice && newState.suppress != null && oldState.suppress !== newState.suppress) {
                queue.setPaused(newState.suppress);
                if (newState.suppress) {
                    newState.guild.members.me!.voice.setRequestToSpeak(true).catch(Util.noop);
                }
            }
        }

        if (!newState.channelId && oldState.channelId === queue.connection.channel.id) {
            if (!Util.isVoiceEmpty(queue.connection.channel)) return;
            const timeout = setTimeout(() => {
                if (!Util.isVoiceEmpty(queue.connection.channel)) return;
                if (!this.queues.has(queue.guild.id)) return;
                if (queue.options.leaveOnEmpty) queue.destroy(true);
                this.emit('channelEmpty', queue);
            }, queue.options.leaveOnEmptyCooldown || 0).unref();
            queue._cooldownsTimeout.set(`empty_${oldState.guild.id}`, timeout);
        }

        if (newState.channelId && newState.channelId === queue.connection.channel.id) {
            const emptyTimeout = queue._cooldownsTimeout.get(`empty_${oldState.guild.id}`);
            const channelEmpty = Util.isVoiceEmpty(queue.connection.channel);
            if (!channelEmpty && emptyTimeout) {
                clearTimeout(emptyTimeout);
                queue._cooldownsTimeout.delete(`empty_${oldState.guild.id}`);
            }
        }

        if (oldState.channelId && newState.channelId && oldState.channelId !== newState.channelId) {
            if (newState.member!.id === newState.guild.members.me!.id) {
                if (queue.connection && newState.member!.id === newState.guild.members.me!.id) queue.connection.channel = newState.channel!;
                const emptyTimeout = queue._cooldownsTimeout.get(`empty_${oldState.guild.id}`);
                const channelEmpty = Util.isVoiceEmpty(queue.connection.channel);
                if (!channelEmpty && emptyTimeout) {
                    clearTimeout(emptyTimeout);
                    queue._cooldownsTimeout.delete(`empty_${oldState.guild.id}`);
                } else {
                    const timeout = setTimeout(() => {
                        if (queue.connection && !Util.isVoiceEmpty(queue.connection.channel)) return;
                        if (!this.queues.has(queue.guild.id)) return;
                        if (queue.options.leaveOnEmpty) queue.destroy(true);
                        this.emit('channelEmpty', queue);
                    }, queue.options.leaveOnEmptyCooldown || 0).unref();
                    queue._cooldownsTimeout.set(`empty_${oldState.guild.id}`, timeout);
                }
            } else {
                if (newState.channelId !== queue.connection.channel.id) {
                    if (!Util.isVoiceEmpty(queue.connection.channel)) return;
                    if (queue._cooldownsTimeout.has(`empty_${oldState.guild.id}`)) return;
                    const timeout = setTimeout(() => {
                        if (!Util.isVoiceEmpty(queue.connection.channel)) return;
                        if (!this.queues.has(queue.guild.id)) return;
                        if (queue.options.leaveOnEmpty) queue.destroy(true);
                        this.emit('channelEmpty', queue);
                    }, queue.options.leaveOnEmptyCooldown || 0).unref();
                    queue._cooldownsTimeout.set(`empty_${oldState.guild.id}`, timeout);
                } else {
                    const emptyTimeout = queue._cooldownsTimeout.get(`empty_${oldState.guild.id}`);
                    const channelEmpty = Util.isVoiceEmpty(queue.connection.channel);
                    if (!channelEmpty && emptyTimeout) {
                        clearTimeout(emptyTimeout);
                        queue._cooldownsTimeout.delete(`empty_${oldState.guild.id}`);
                    }
                }
            }
        }
    }

    /**
     * Creates a queue for a guild if not available, else returns existing queue
     * @param {GuildResolvable} guild The guild
     * @param {PlayerOptions} queueInitOptions Queue init options
     * @returns {Queue}
     */
    createQueue<T = unknown>(guild: GuildResolvable, queueInitOptions: PlayerOptions & { metadata?: T } = {}): Queue<T> {
        guild = this.client.guilds.resolve(guild)!;
        if (!guild) throw new PlayerError('Unknown Guild', ErrorStatusCode.UNKNOWN_GUILD);
        if (this.queues.has(guild.id)) return this.queues.get(guild.id) as Queue<T>;

        const _meta = queueInitOptions.metadata;
        delete queueInitOptions['metadata'];
        queueInitOptions.volumeSmoothness ??= this.options.smoothVolume ? 0.08 : 0;
        queueInitOptions.ytdlOptions ??= this.options.ytdlOptions;
        const queue = new Queue(this, guild, queueInitOptions);
        queue.metadata = _meta;
        this.queues.set(guild.id, queue);

        return queue as Queue<T>;
    }

    /**
     * Returns the queue if available
     * @param {GuildResolvable} guild The guild id
     * @returns {Queue | undefined}
     */
    getQueue<T = unknown>(guild: GuildResolvable): Queue<T> | undefined {
        guild = this.client.guilds.resolve(guild)!;
        if (!guild) throw new PlayerError('Unknown Guild', ErrorStatusCode.UNKNOWN_GUILD);
        return this.queues.get(guild.id) as Queue<T>;
    }

    /**
     * Deletes a queue and returns deleted queue object
     * @param {GuildResolvable} guild The guild id to remove
     * @returns {Queue}
     */
    deleteQueue<T = unknown>(guild: GuildResolvable) {
        guild = this.client.guilds.resolve(guild)!;
        if (!guild) throw new PlayerError('Unknown Guild', ErrorStatusCode.UNKNOWN_GUILD);
        const prev = this.getQueue<T>(guild)!;

        try {
            prev.destroy();
        } catch {} // eslint-disable-line no-empty
        this.queues.delete(guild.id);

        return prev;
    }

    /**
     * @typedef {object} PlayerSearchResult
     * @property {Playlist} [playlist] The playlist (if any)
     * @property {Track[]} tracks The tracks
     */
    /**
     * Search tracks
     * @param {string|Track} query The search query
     * @param {SearchOptions} options The search options
     * @returns {Promise<SearchResult>}
     */
    async search(query: string | Track, options: SearchOptions): Promise<SearchResult> {
        if (options.requestedBy != null) options.requestedBy = this.client.users.resolve(options.requestedBy)!;
        if (query instanceof Track)
            return new SearchResult(this, {
                playlist: query.playlist || null,
                tracks: [query],
                query: query.toString(),
                extractor: null,
                queryType: QueryType.AUTO,
                requestedBy: options.requestedBy
            });
        if (!options) throw new PlayerError('DiscordPlayer#search needs search options!', ErrorStatusCode.INVALID_ARG_TYPE);

        let extractor: BaseExtractor | null = null;

        options.searchEngine ??= QueryType.AUTO;

        const queryType = options.searchEngine === QueryType.AUTO ? QueryResolver.resolve(query) : options.searchEngine;

        if (options.searchEngine.startsWith('ext:')) {
            extractor = this.extractors.get(options.searchEngine.substring(4))!;
            if (!extractor) return new SearchResult(this, { query, queryType });
        }

        if (!extractor) {
            extractor = (await this.extractors.run((ext) => ext.validate(query, queryType as SearchQueryType)))?.extractor || null;
        }

        // no extractors available
        if (!extractor) {
            return new SearchResult(this, { query, queryType });
        }

        const res = await extractor
            .handle(query, {
                type: queryType as SearchQueryType,
                requestedBy: options.requestedBy as User
            })
            .catch(() => null);

        if (res) {
            return new SearchResult(this, {
                query,
                queryType,
                playlist: res.playlist,
                tracks: res.tracks,
                extractor
            });
        }

        const result = await this.extractors.run(
            async (ext) =>
                (await ext.validate(query)) &&
                ext.handle(query, {
                    type: queryType as SearchQueryType,
                    requestedBy: options.requestedBy as User
                })
        );
        if (!result?.result) return new SearchResult(this, { query, queryType });

        return new SearchResult(this, {
            query,
            queryType,
            playlist: result.result.playlist,
            tracks: result.result.tracks,
            extractor: result.extractor
        });
    }

    /**
     * Generates a report of the dependencies used by the `@discordjs/voice` module. Useful for debugging.
     * @returns {string}
     */
    scanDeps() {
        const line = '-'.repeat(50);
        const depsReport = generateDependencyReport();
        const extractorReport = this.extractors.store
            .map((m) => {
                return m.identifier;
            })
            .join('\n');
        return `${depsReport}\n${line}\nLoaded Extractors:\n${extractorReport || 'None'}`;
    }

    emit<U extends keyof PlayerEvents>(eventName: U, ...args: Parameters<PlayerEvents[U]>): boolean {
        if (this.requiredEvents.includes(eventName) && !super.eventNames().includes(eventName)) {
            // eslint-disable-next-line no-console
            console.error(...args);
            process.emitWarning(`[DiscordPlayerWarning] Unhandled "${eventName}" event! Events ${this.requiredEvents.map((m) => `"${m}"`).join(', ')} must have event listeners!`);
            return false;
        } else {
            return super.emit(eventName, ...args);
        }
    }

    /**
     * Resolves queue
     * @param {GuildResolvable|Queue} queueLike Queue like object
     * @returns {Queue}
     */
    resolveQueue<T>(queueLike: GuildResolvable | Queue): Queue<T> {
        return this.getQueue(queueLike instanceof Queue ? queueLike.guild : queueLike)!;
    }

    *[Symbol.iterator]() {
        yield* Array.from(this.queues.values());
    }

    /**
     * Creates `Playlist` instance
     * @param data The data to initialize a playlist
     */
    createPlaylist(data: PlaylistInitData) {
        return new Playlist(this, data);
    }
}

export { Player };
