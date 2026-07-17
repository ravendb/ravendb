import { useEffect, useState } from "react";

interface DumpFileCollectionsState {
    collections: string[];
    isReading: boolean;
    readError: string;
}

const initialState: DumpFileCollectionsState = {
    collections: [],
    isReading: false,
    readError: null,
};

// Matches "@collection":"<name>" including escaped characters inside the name
const collectionRegex = /"@collection"\s*:\s*"((?:[^"\\]|\\.)*)"/g;

// Keep a tail between chunks so a match split across chunk boundary is not lost
const chunkOverlap = 256;

async function scanStream(stream: ReadableStream<Uint8Array>, signal: AbortSignal): Promise<string[]> {
    const decoder = new TextDecoder();
    const reader = stream.getReader();
    const found = new Set<string>();
    let carry = "";

    // eslint-disable-next-line no-constant-condition
    while (true) {
        if (signal.aborted) {
            await reader.cancel();
            throw new DOMException("Aborted", "AbortError");
        }

        const { done, value } = await reader.read();
        if (done) {
            break;
        }

        const text = carry + decoder.decode(value, { stream: true });

        let match: RegExpExecArray;
        collectionRegex.lastIndex = 0;
        while ((match = collectionRegex.exec(text)) !== null) {
            try {
                found.add(JSON.parse(`"${match[1]}"`));
            } catch {
                // malformed escape sequence - skip
            }
        }

        carry = text.slice(-chunkOverlap);
    }

    return Array.from(found).sort((a, b) => a.localeCompare(b));
}

async function readCollectionsFromDumpFile(file: File, signal: AbortSignal): Promise<string[]> {
    const header = new Uint8Array(await file.slice(0, 2).arrayBuffer());
    const isGzip = header.length === 2 && header[0] === 0x1f && header[1] === 0x8b;

    const stream = isGzip
        ? file.stream().pipeThrough(new DecompressionStream("gzip"))
        : file.stream();

    return scanStream(stream, signal);
}

export function useDumpFileCollections(file: File | null): DumpFileCollectionsState {
    const [state, setState] = useState<DumpFileCollectionsState>(initialState);

    useEffect(() => {
        if (!file) {
            setState(initialState);
            return;
        }

        const abortController = new AbortController();
        setState({ collections: [], isReading: true, readError: null });

        readCollectionsFromDumpFile(file, abortController.signal)
            .then((collections) => {
                if (!abortController.signal.aborted) {
                    setState({ collections, isReading: false, readError: null });
                }
            })
            .catch((error) => {
                if (!abortController.signal.aborted) {
                    setState({
                        collections: [],
                        isReading: false,
                        readError:
                            "Could not read the collection list from the selected file" +
                            (error instanceof Error && error.message ? ` (${error.message})` : ""),
                    });
                }
            });

        return () => abortController.abort();
    }, [file]);

    return state;
}
