export interface Codec {
  encode(value: unknown): string;
  decode(raw: string): any;
}

/** Plain strings, compatible with Redisson StringCodec. */
export class StringCodec implements Codec {
  encode(value: unknown): string {
    return String(value);
  }
  decode(raw: string): string {
    return raw;
  }
}

/** JSON, compatible with Redisson JsonJacksonCodec (without @class type info). */
export class JsonCodec implements Codec {
  encode(value: unknown): string {
    return JSON.stringify(value);
  }
  decode(raw: string): any {
    return JSON.parse(raw);
  }
}
