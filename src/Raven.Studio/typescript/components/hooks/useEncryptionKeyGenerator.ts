import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";

export function useEncryptionKeyGenerator(currentKey: string, onGenerate: (key: string) => void) {
    const { databasesService } = useServices();

    const asyncGenerate = useAsync(
        async (isReplacingKey: boolean) => {
            if (!isReplacingKey && currentKey) {
                return;
            }

            onGenerate(await databasesService.generateSecret());
        },
        [false]
    );

    return {
        isGenerating: asyncGenerate.loading,
        regenerate: () => asyncGenerate.execute(true),
    };
}
