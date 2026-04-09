declare module 'can-ndjson-stream' {
  function ndjsonStream(body: ReadableStream | null): Promise<ReadableStream>;
  export = ndjsonStream;
}
