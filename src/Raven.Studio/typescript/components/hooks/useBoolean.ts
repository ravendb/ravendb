import { useCallback, useState } from "react";

const useBoolean = (initial: boolean) => {
    const [value, setValue] = useState(initial);
    return {
        value,
        setValue,
        toggle: useCallback(() => setValue((value: any) => !value), []),
        setTrue: useCallback(() => setValue(true), []),
        setFalse: useCallback(() => setValue(false), []),
    };
};

export default useBoolean;
