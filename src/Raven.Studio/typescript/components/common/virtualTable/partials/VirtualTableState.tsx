import { ReactNode } from "react";
import Spinner from "react-bootstrap/Spinner";
import { EmptySet } from "../../EmptySet";

export interface VirtualTableStateProps {
    isLoading: boolean;
    isEmpty: boolean;
    emptyMessage?: ReactNode;
}

export function VirtualTableState(props: VirtualTableStateProps) {
    const { isLoading, isEmpty, emptyMessage = "No results" } = props;

    return (
        <>
            {isLoading && <Spinner className="spinner-gradient table-state" data-testid="loader" />}
            {isEmpty && !isLoading && (
                <div className="table-state">
                    <EmptySet compact>{emptyMessage}</EmptySet>
                </div>
            )}
        </>
    );
}
