import { SelectOption } from "components/common/select/Select";
import { useAsyncDebounce } from "components/hooks/useAsyncDebounce";
import { useServices } from "components/hooks/useServices";

export function useOpenAiModelOptions(props: {
    apiKey: string;
    projectId?: string;
    organizationId?: string;
    endpoint?: string;
}) {
    const { tasksService } = useServices();

    return useAsyncDebounce(
        async () => {
            const apiKey = props.apiKey?.trim() ?? "";
            const projectId = props.projectId?.trim() ?? "";
            const organizationId = props.organizationId?.trim() ?? "";
            const endpoint = props.endpoint?.trim() ?? "";

            if (!apiKey) {
                return [];
            }

            try {
                const result = await tasksService.getOpenAiModels(apiKey, projectId, organizationId, endpoint);
                return [...result].sort().map((x) => ({ label: x, value: x }) satisfies SelectOption);
            } catch {
                return [];
            }
        },
        [props.apiKey, props.projectId, props.organizationId, props.endpoint],
        300
    );
}
