import { useState } from "react";

const useId = (prefix: string) => {
    const [id] = useState(() => _.uniqueId(prefix));

    return id;
};

export default useId;
