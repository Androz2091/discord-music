/* eslint-disable @typescript-eslint/no-explicit-any */

import { Buffer } from 'buffer';
import { Exceptions } from '../errors';
import { Playlist, SerializedPlaylist, type SerializedTrack, Track } from '../fabric';
import { Player } from '../Player';
import { TypeUtil } from './TypeUtil';

export enum SerializedType {
    Track = 'track',
    Playlist = 'playlist'
}

export type Encodable = SerializedTrack | SerializedPlaylist;

const isTrack = (data: any): data is SerializedTrack => data.$type === SerializedType.Track;
const isPlaylist = (data: any): data is SerializedPlaylist => data.$type === SerializedType.Playlist;

export function serialize(data: Track | Playlist | any) {
    if (data instanceof Track) return data.serialize();
    if (data instanceof Playlist) return data.serialize();

    try {
        return data.toJSON();
    } catch {
        throw Exceptions.ERR_SERIALIZATION_FAILED();
    }
}

export function deserialize(player: Player, data: Encodable) {
    if (isTrack(data)) return Track.fromSerialized(player, data);
    if (isPlaylist(data)) return Playlist.fromSerialized(player, data);

    throw Exceptions.ERR_DESERIALIZATION_FAILED();
}

export function encode(data: Encodable) {
    const str = JSON.stringify(data);

    return Buffer.from(str).toString('base64');
}

export function decode(data: string) {
    const str = Buffer.from(data, 'base64').toString();

    return JSON.parse(str);
}

export function tryIntoThumbnailString(data: any) {
    if (!data) return null;
    try {
        if (TypeUtil.isString(data)) return data;
        return data?.url ?? data?.thumbnail?.url ?? null;
    } catch {
        return null;
    }
}
