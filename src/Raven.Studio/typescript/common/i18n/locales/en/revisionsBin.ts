export default {
    selectionLabel: "Selection",
    dataChanged: "The data has changed. Your results may contain duplicates or stale entries.",
    emptyBin: "The revisions bin is empty.",
    columns: {
        id: "Id",
        changeVector: "Change Vector",
        deletionDate: "Deletion date",
    },
    deleteConfirm: {
        title: "Delete Revisions?",
        message: "The selected \"Delete Revision\" items will be removed,<br/>and all their associated revisions will be permanently deleted.<br/><br/>This action cannot be undone.",
        confirmButton: "Yes, delete",
    },
} as const;
