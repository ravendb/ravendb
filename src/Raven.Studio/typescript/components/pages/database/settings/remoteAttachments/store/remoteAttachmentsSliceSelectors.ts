import { RootState } from "components/store";
import { destinationsSelectors, initialDestinationsSelectors } from "./remoteAttachmentsSlice";
import { isEqual, omit } from "lodash";
import { RemoteAttachmentsDestinationFormData } from "components/pages/database/settings/remoteAttachments/remoteAttachmentsValidation";

const loadStatus = (s: RootState) => s.remoteAttachments.loadStatus;

const areDestinationsDifferent = <T extends object>(current: T, initial: T) =>
    !isEqual(omit(current, ["s3.isEnabled", "azure.isEnabled"]), omit(initial, ["s3.isEnabled", "azure.isEnabled"]));

const isAnyModified = (s: RootState) => {
    const current = destinationsSelectors.selectAll(s.remoteAttachments);
    const initial = initialDestinationsSelectors.selectAll(s.remoteAttachments);

    return areDestinationsDifferent(current, initial);
};

const selectDestinations = (s: RootState): RemoteAttachmentsDestinationFormData[] =>
    destinationsSelectors.selectAll(s.remoteAttachments);
const selectDestinationsTotal = (s: RootState) => destinationsSelectors.selectTotal(s.remoteAttachments);

const selectIsDestinationModified = (id: string) => (s: RootState) => {
    const current = destinationsSelectors.selectById(s.remoteAttachments, id);
    const initial = initialDestinationsSelectors.selectById(s.remoteAttachments, id);

    return areDestinationsDifferent(current, initial);
};

export const remoteAttachmentsSelectors = {
    loadStatus,
    isAnyModified,

    destinations: selectDestinations,
    destinationsTotal: selectDestinationsTotal,
    isDestinationModified: selectIsDestinationModified,
};
