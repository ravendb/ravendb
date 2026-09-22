import { globalDispatch } from "components/storeCompat";
import { Collection, collectionsTrackerActions } from "components/common/shell/collectionsTrackerSlice";

const defaultCollections: Collection[] = [
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
];

export class MockCollectionsTracker {
    with_Collections(overrides: Partial<Collection>[] = []) {
        const collections = defaultCollections.map((collection) => ({
            ...collection,
            ...overrides.find((x) => x.name === collection.name),
        }));

        globalDispatch(collectionsTrackerActions.collectionsLoaded(collections));
    }

    with_CollectionsExcept(removedNames: string[]) {
        globalDispatch(
            collectionsTrackerActions.collectionsLoaded(
                defaultCollections.filter((x) => !removedNames.includes(x.name))
            )
        );
    }
}
