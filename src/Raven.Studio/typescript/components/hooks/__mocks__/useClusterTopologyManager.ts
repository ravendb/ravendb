import { useEffect, useState } from "react";
import { MockClusterTopologyManager } from "test/mocks/hooks/MockClusterTopologyManager";

const mockManager = new MockClusterTopologyManager();

export function useClusterTopologyManager() {
    const [state, setState] = useState(mockManager.state());

    useEffect(() => {
        const sub = mockManager.state.subscribe(setState);

        return () => sub.dispose();
    }, []);

    return {
        localTag: state.localTag,
        nodeTags: state.nodeTags,
    };
}
