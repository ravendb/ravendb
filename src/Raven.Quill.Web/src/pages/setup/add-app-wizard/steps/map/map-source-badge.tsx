import { Badge } from "@/components/shadcn/ui/badge";
import { getOptionLabel } from "@/lib/form-utils";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import { MAP_SOURCE_OPTIONS } from "@/pages/setup/add-app-wizard/steps/map/map-source-options";

export function MapSourceBadge() {
    const source = useSetupWizardStore((state) => state.mapOrigin?.source);

    if (!source) {
        return null;
    }

    return <Badge variant="primary">{getOptionLabel(MAP_SOURCE_OPTIONS, source)}</Badge>;
}
