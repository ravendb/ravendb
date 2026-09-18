import { useEffect, useRef, useState } from "react";
import { isImageFile } from "components/common/fileTypeBadge";

const maxPreviewSizeInBytes = 5 * 1024 * 1024;

function isPreviewable(file: File): boolean {
    return isImageFile(file.name) && file.size <= maxPreviewSizeInBytes;
}

export default function useFilePreviews(files: File[]): Record<string, string> {
    const [previews, setPreviews] = useState<Record<string, string>>({});
    const urlsByNameRef = useRef<Record<string, string>>({});

    const signature = files.map((file) => file.name).join("|");

    useEffect(() => {
        const current = urlsByNameRef.current;
        const wanted = files.filter(isPreviewable);
        const wantedNames = new Set(wanted.map((file) => file.name));

        for (const name of Object.keys(current)) {
            if (!wantedNames.has(name)) {
                URL.revokeObjectURL(current[name]);
                delete current[name];
            }
        }

        for (const file of wanted) {
            if (!current[file.name]) {
                current[file.name] = URL.createObjectURL(file);
            }
        }

        setPreviews({ ...current });
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [signature]);

    useEffect(() => {
        const current = urlsByNameRef.current;

        return () => {
            Object.values(current).forEach(URL.revokeObjectURL);
        };
    }, []);

    return previews;
}
