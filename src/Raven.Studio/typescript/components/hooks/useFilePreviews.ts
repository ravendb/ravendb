import { useEffect, useRef, useState } from "react";
import { isImageFile } from "components/common/fileTypeBadge";

const maxPreviewSizeInBytes = 5 * 1024 * 1024;

function isPreviewable(file: File): boolean {
    return isImageFile(file.name) && file.size <= maxPreviewSizeInBytes;
}

export function getFilePreviewKey(file: File): string {
    return `${file.name}|${file.size}|${file.lastModified}`;
}

export default function useFilePreviews(files: File[]): Record<string, string> {
    const [previews, setPreviews] = useState<Record<string, string>>({});
    const urlsByKeyRef = useRef<Record<string, string>>({});

    const signature = files.map(getFilePreviewKey).join("~");

    useEffect(() => {
        const current = urlsByKeyRef.current;
        const wanted = files.filter(isPreviewable);
        const wantedKeys = new Set(wanted.map(getFilePreviewKey));

        for (const key of Object.keys(current)) {
            if (!wantedKeys.has(key)) {
                URL.revokeObjectURL(current[key]);
                delete current[key];
            }
        }

        for (const file of wanted) {
            const key = getFilePreviewKey(file);
            if (!current[key]) {
                current[key] = URL.createObjectURL(file);
            }
        }

        setPreviews({ ...current });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [signature]);

    useEffect(() => {
        const current = urlsByKeyRef.current;

        return () => {
            Object.values(current).forEach(URL.revokeObjectURL);
        };
    }, []);

    return previews;
}
