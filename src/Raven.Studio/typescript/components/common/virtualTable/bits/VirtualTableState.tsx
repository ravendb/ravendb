import { Spinner } from "reactstrap";
import { EmptySet } from "../../EmptySet";

export interface VirtualTableStateProps {
    isLoading: boolean;
    isEmpty: boolean;
}

export function VirtualTableState(props: VirtualTableStateProps) {
    const { isLoading, isEmpty } = props;

    return (
        <>
            {isLoading && <Spinner className="spinner-gradient table-state" data-testid="loader" />}
            {isEmpty && !isLoading && (
                <div className="table-state">
                    <EmptySet>No results</EmptySet>
                </div>
            )}
        </>
    );
}
