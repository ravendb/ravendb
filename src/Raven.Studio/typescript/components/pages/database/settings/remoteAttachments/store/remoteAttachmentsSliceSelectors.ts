import { RootState } from "components/store";
import { destinationsSelectors, initialDestinationsSelectors } from "./remoteAttachmentsSlice";

const loadStatus = (s: RootState) => s.remoteAttachments.loadStatus;
const isAnyModified = (s: RootState) =>
    !_.isEqual(
        destinationsSelectors.selectAll(s.remoteAttachments),
        initialDestinationsSelectors.selectAll(s.remoteAttachments)
    );

const selectDestinations = (s: RootState) => destinationsSelectors.selectAll(s.remoteAttachments);
const selectDestinationsTotal = (s: RootState) => destinationsSelectors.selectTotal(s.remoteAttachments);

const selectIsDestinationModified = (id: string) => (s: RootState) => {
    const current = destinationsSelectors.selectById(s.remoteAttachments, id);
    const initial = initialDestinationsSelectors.selectById(s.remoteAttachments, id);
    return !_.isEqual(current, initial);
};

export const remoteAttachmentsSelectors = {
    loadStatus,
    isAnyModified,

    destinations: selectDestinations,
    destinationsTotal: selectDestinationsTotal,
    isDestinationModified: selectIsDestinationModified,
};
