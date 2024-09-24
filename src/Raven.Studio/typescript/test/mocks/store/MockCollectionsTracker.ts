import { globalDispatch } from "components/storeCompat";
import { collectionsTrackerActions } from "components/common/shell/collectionsTrackerSlice";

export class MockCollectionsTracker {
    with_Collections() {
        globalDispatch(
            collectionsTrackerActions.collectionsLoaded([
                {
                    name: "All Documents",
                    countPrefix: "116",
                    documentCount: 116,
                    hasBounceClass: false,
                    sizeClass: "",
                    lastDocumentChangeVector: null,
                },
                {
                    name: "@hilo",
                    countPrefix: "8",
                    documentCount: 8,
                    hasBounceClass: false,
                    sizeClass: "",
                    lastDocumentChangeVector: null,
                },
                {
                    name: "Categories",
                    countPrefix: "8",
                    documentCount: 8,
                    hasBounceClass: false,
                    sizeClass: "",
                    lastDocumentChangeVector: null,
                },
                {
                    name: "Companies",
                    countPrefix: "91",
                    documentCount: 91,
                    hasBounceClass: false,
                    sizeClass: "",
                    lastDocumentChangeVector: null,
                },
                {
                    name: "Shippers",
                    countPrefix: "9",
                    documentCount: 9,
                    hasBounceClass: false,
                    sizeClass: "",
                    lastDocumentChangeVector: null,
                },
                {
                    name: "Orders",
                    countPrefix: "128",
                    documentCount: 128,
                    hasBounceClass: false,
                    sizeClass: "",
                    lastDocumentChangeVector: null,
                },
            ])
        );
    }
}
