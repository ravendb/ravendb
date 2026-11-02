type RevisionType = Raven.Server.Documents.Revisions.RevisionsStorage.RevisionType;

export const allRevisionsUtils = {
    smallSampleSize: 100, // we only want some preview
    isSmallSample: (type: RevisionType, collectionName: string) => type !== "All" && !!collectionName,
};
