
export function getReactDirtyFlag(dirtyFlag: () => DirtyFlag, customDiscardStayResult: KnockoutObservable<() => JQueryDeferred<confirmDialogResult>>): ReactDirtyFlag {
    return {
        setIsDirty: (value: boolean, customDialog?: () => JQueryDeferred<confirmDialogResult>) => {
            if (value) {
                dirtyFlag().forceDirty();
                customDiscardStayResult(customDialog);
            } else {
                dirtyFlag().reset();
                customDiscardStayResult(null);
            }
        }
    };
}
