export function getReactDirtyFlag(dirtyFlag: () => DirtyFlag): ReactDirtyFlag {
    return {
        setIsDirty: (value: boolean) => value
            ? dirtyFlag().forceDirty()
            : dirtyFlag().reset(),
    };
}
