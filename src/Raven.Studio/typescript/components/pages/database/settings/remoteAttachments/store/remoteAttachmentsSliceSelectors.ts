import { RootState } from "components/store";
import { destinationsSelectors } from "./remoteAttachmentsSlice";

const loadStatus = (s: RootState) => s.remoteAttachments.loadStatus;
const isAnyModified = (s: RootState) => !_.isEqual(destinationsSelectors.selectAll(s.remoteAttachments), s.remoteAttachments.initialDestinations);

const selectDestinations = (s: RootState) => destinationsSelectors.selectAll(s.remoteAttachments);
const selectDestinationById = (id: string) => (s: RootState) => destinationsSelectors.selectById(s.remoteAttachments, id);
const selectDestinationIds = (s: RootState) => destinationsSelectors.selectIds(s.remoteAttachments);
const selectDestinationsTotal = (s: RootState) => destinationsSelectors.selectTotal(s.remoteAttachments);

const selectIsDestinationModified = (id: string) => (s: RootState) => {
    const current = destinationsSelectors.selectById(s.remoteAttachments, id);
    const initial = s.remoteAttachments.initialDestinations?.find((d) => d.identifier === id);
    return !_.isEqual(current, initial);
};

export const remoteAttachmentsSelectors = {
    loadStatus,
    isAnyModified,

    destinations: selectDestinations,
    destinationById: selectDestinationById,
    destinationIds: selectDestinationIds,
    destinationsTotal: selectDestinationsTotal,
    isDestinationModified: selectIsDestinationModified,
};
