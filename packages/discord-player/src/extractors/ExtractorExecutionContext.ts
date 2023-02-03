import { Player } from '../Player';
import { Collection } from '@discord-player/utils';
import { BaseExtractor } from './BaseExtractor';

export class ExtractorExecutionContext {
    public store = new Collection<string, BaseExtractor>();
    public constructor(public player: Player) {}

    public isRegistered(identifier: string) {
        return this.store.has(identifier);
    }

    public get size() {
        return this.store.size;
    }

    public get(identifier: string) {
        return this.store.get(identifier);
    }

    public async register(_extractor: typeof BaseExtractor) {
        if (typeof _extractor.identifier !== 'string' || this.store.has(_extractor.identifier)) return;
        const extractor = new _extractor(this);

        try {
            this.store.set(_extractor.identifier, extractor);
            await extractor.activate();
        } catch {
            this.store.delete(_extractor.identifier);
        }
    }

    public async unregister<K extends string | BaseExtractor>(_extractor: K) {
        const extractor = typeof _extractor === 'string' ? this.store.get(_extractor) : this.store.find((r) => r === _extractor);
        if (!extractor) return;

        try {
            const key = extractor.identifier || this.store.findKey((e) => e === extractor)!;
            this.store.delete(key);
            await extractor.deactivate();
        } catch {
            // do nothing
        }
    }

    public async unregisterAll() {
        try {
            await Promise.all(
                this.store.map((e, k) => {
                    this.store.delete(k);
                    return e.deactivate();
                })
            );
        } catch {
            // do nothing
        }
    }

    public async run<T = unknown>(fn: ExtractorExecutionFN<T>) {
        for (const ext of this.store.values()) {
            const result = await fn(ext).catch(() => {
                return false;
            });
            if (result)
                return {
                    extractor: ext,
                    result
                } as ExtractorExecutionResult<T>;
        }

        return null;
    }
}

export interface ExtractorExecutionResult<T = unknown> {
    extractor: BaseExtractor;
    result: T;
}
export type ExtractorExecutionFN<T = unknown> = (extractor: BaseExtractor) => Promise<T | boolean>;
