/**
 * Codec layer — wire-compatible with Java Redisson JsonJacksonCodec
 * (default typing) and with redi.py / redi.go.
 *
 * Encode rules (verified against Redisson 4.6.1):
 *  - string / boolean / null / small int (int32 range) / float → bare JSON
 *  - int outside int32 range → ["java.lang.Long", v]
 *  - plain object → {"@class":"java.util.LinkedHashMap", ...} (recursive)
 *  - array → ["java.util.ArrayList",[...]] (recursive)
 *
 * Decode rules:
 *  - ["java.lang.*", v] → v ; ["java.util.*", v] → v (recursive)
 *  - strip any "@class" key (recursive)
 *  - non-JSON payload → raw string unchanged
 */

export interface Codec {
  encode(value: unknown): string;
  decode(raw: string | null | undefined): any;
}

const INT32_MIN = -2147483648;
const INT32_MAX = 2147483647;

const COLLECTION_TYPES = new Set([
  'java.util.ArrayList',
  'java.util.LinkedList',
  'java.util.HashMap',
  'java.util.LinkedHashMap',
  'java.util.TreeMap',
  'java.util.HashSet',
  'java.util.LinkedHashSet',
  'java.util.TreeSet',
]);

function isPlainObject(v: unknown): v is Record<string, unknown> {
  if (v === null || typeof v !== 'object') return false;
  if (Array.isArray(v)) return false;
  const proto = Object.getPrototypeOf(v);
  return proto === Object.prototype || proto === null;
}

export function javaTypeWrap(v: any): any {
  if (Array.isArray(v)) {
    return ['java.util.ArrayList', v.map(javaTypeWrap)];
  }
  if (isPlainObject(v)) {
    const out: Record<string, unknown> = {};
    for (const k of Object.keys(v)) out[k] = javaTypeWrap((v as any)[k]);
    if (!('@class' in out)) out['@class'] = 'java.util.LinkedHashMap';
    return out;
  }
  return v;
}

export function unwrap(v: any): any {
  if (Array.isArray(v)) {
    if (v.length === 2 && typeof v[0] === 'string') {
      if (v[0].startsWith('java.lang.') || COLLECTION_TYPES.has(v[0]) || v[0].startsWith('java.util.Collections$Unmo')) {
        return unwrap(v[1]);
      }
    }
    for (let i = 0; i < v.length; i++) {
      const e = v[i];
      if (e !== null && typeof e === 'object') v[i] = unwrap(e);
    }
    return v;
  }
  if (v !== null && typeof v === 'object') {
    delete (v as any)['@class'];
    for (const k of Object.keys(v)) {
      const e = (v as any)[k];
      if (e !== null && typeof e === 'object') (v as any)[k] = unwrap(e);
    }
    return v;
  }
  return v;
}

/** Plain strings, compatible with Redisson StringCodec. */
export class StringCodec implements Codec {
  encode(value: unknown): string {
    return String(value);
  }
  decode(raw: string | null | undefined): any {
    if (raw === null || raw === undefined) return null;
    return raw;
  }
}

/** Raw bytes passthrough (RBinaryStream / ByteArrayCodec). No codec applied. */
export class ByteArrayCodec implements Codec {
  encode(value: unknown): string {
    if (typeof value === 'string') return value;
    if (Buffer.isBuffer(value)) return (value as Buffer).toString('latin1');
    if (value instanceof Uint8Array) return Buffer.from(value).toString('latin1');
    return String(value);
  }
  decode(raw: string | null | undefined): any {
    if (raw === null || raw === undefined) return null;
    return raw;
  }
}

/** JSON, compatible with Redisson JsonJacksonCodec default typing. */
export interface JsonCodecOptions {
  /** Decode integer literals outside ±(2^53-1) as BigInt instead of the
   * precision-lossy JS number. Default false keeps the historical number
   * contract. Encoding BigInt is always lossless. */
  bigIntForUnsafeLong?: boolean;
}

function restoreBigInts(value: any, table: Map<string, bigint>): any {
  if (typeof value === 'string') return table.get(value) ?? value;
  if (Array.isArray(value)) {
    for (let i = 0; i < value.length; i++) value[i] = restoreBigInts(value[i], table);
    return value;
  }
  if (value !== null && typeof value === 'object') {
    for (const k of Object.keys(value)) value[k] = restoreBigInts(value[k], table);
  }
  return value;
}

/** Replace unsafe integer literals outside JSON strings with unique string
 * tokens, so JSON.parse cannot round them before we restore them as BigInt. */
function protectUnsafeInts(raw: string): { text: string; table: Map<string, bigint> } {
  const table = new Map<string, bigint>();
  const nonce = `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`;
  let out = '';
  let i = 0;
  let inString = false;
  let escaped = false;
  while (i < raw.length) {
    const ch = raw[i];
    if (inString) {
      out += ch;
      if (escaped) escaped = false;
      else if (ch === '\\') escaped = true;
      else if (ch === '"') inString = false;
      i++;
      continue;
    }
    if (ch === '"') {
      inString = true;
      out += ch;
      i++;
      continue;
    }
    if (ch === '-' || (ch >= '0' && ch <= '9')) {
      let j = i + 1;
      while (j < raw.length && /[0-9eE+.\-]/.test(raw[j])) j++;
      const token = raw.slice(i, j);
      if (/^-?\d+$/.test(token) && !Number.isSafeInteger(Number(token))) {
        const key = `__redi_bigint_${nonce}_${table.size}__`;
        table.set(key, BigInt(token));
        out += JSON.stringify(key);
      } else {
        out += token;
      }
      i = j;
      continue;
    }
    out += ch;
    i++;
  }
  return { text: out, table };
}

export class JsonCodec implements Codec {
  private readonly bigIntForUnsafeLong: boolean;

  constructor(opts: JsonCodecOptions = {}) {
    this.bigIntForUnsafeLong = opts.bigIntForUnsafeLong === true;
  }

  private stringifyTyped(value: unknown): string {
    const nonce = `${Date.now().toString(36)}${Math.random().toString(36).slice(2)}`;
    const token = `__redi_bigint_enc_${nonce}_`;
    const bigints: string[] = [];
    const json = JSON.stringify(javaTypeWrap(value), (_k, v) => {
      if (typeof v === 'bigint') {
        const key = `${token}${bigints.length}`;
        bigints.push(v.toString());
        return key;
      }
      return v;
    });
    if (json === undefined) return 'null';
    if (bigints.length === 0) return json;
    return json.replace(new RegExp(`"${token}(\\d+)"`, 'g'), (_m, idx) => `["java.lang.Long",${bigints[Number(idx)]}]`);
  }

  encode(value: unknown): string {
    // Long wrap: integers outside int32 (bool excluded)
    if (typeof value === 'number' && Number.isInteger(value)) {
      if (value > INT32_MAX || value < INT32_MIN) {
        return JSON.stringify(['java.lang.Long', value]);
      }
      return JSON.stringify(value);
    }
    if (typeof value === 'bigint') {
      // Never route BigInt through Number(): snowflake ids and Long.MAX_VALUE
      // must keep every decimal digit.
      return `["java.lang.Long",${value.toString()}]`;
    }
    if (value === null || typeof value === 'string' || typeof value === 'boolean' || typeof value === 'number') {
      return JSON.stringify(value);
    }
    return this.stringifyTyped(value);
  }
  decode(raw: string | null | undefined): any {
    if (raw === null || raw === undefined) return null;
    if (raw === '') return null;
    try {
      if (this.bigIntForUnsafeLong) {
        const { text, table } = protectUnsafeInts(raw);
        const parsed = JSON.parse(text);
        return unwrap(restoreBigInts(parsed, table));
      }
      return unwrap(JSON.parse(raw));
    } catch {
      return raw;
    }
  }
}

/** Default shared JSON codec instance. */
export const defaultCodec = new JsonCodec();
