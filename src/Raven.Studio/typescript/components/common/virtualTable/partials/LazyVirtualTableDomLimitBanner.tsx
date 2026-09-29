import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppSelector } from "components/store";
import Button from "react-bootstrap/Button";

interface LazyVirtualTableDomLimitBannerProps {
    itemsName: string;
    onTurnOnPagination: () => void;
}

export default function LazyVirtualTableDomLimitBanner({
    itemsName,
    onTurnOnPagination,
}: LazyVirtualTableDomLimitBannerProps) {
    const isSharded = useAppSelector(databaseSelectors.activeDatabase)?.isSharded;

    return (
        <div className="floating-bar text-nowrap" data-testid="dom-limit-banner">
            <span>
                It looks like you&apos;ve reached the end of the DOM.{" "}
                {isSharded ? (
                    <>Use a query to find more {itemsName}</>
                ) : (
                    <>
                        <Button variant="link" className="p-0 align-baseline" onClick={onTurnOnPagination}>
                            Turn on pagination
                        </Button>{" "}
                        to fetch more {itemsName}
                    </>
                )}
            </span>
        </div>
    );
}
