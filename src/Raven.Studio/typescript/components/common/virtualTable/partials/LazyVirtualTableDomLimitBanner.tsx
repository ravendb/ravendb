import Button from "react-bootstrap/Button";

interface LazyVirtualTableDomLimitBannerProps {
    itemsName: string;
    canPaginate: boolean;
    onTurnOnPagination: () => void;
}

export default function LazyVirtualTableDomLimitBanner({
    itemsName,
    canPaginate,
    onTurnOnPagination,
}: LazyVirtualTableDomLimitBannerProps) {
    return (
        <div className="floating-bar text-nowrap" data-testid="dom-limit-banner">
            <span>
                It looks like you&apos;ve reached the end of the DOM.{" "}
                {canPaginate ? (
                    <>
                        <Button variant="link" className="p-0 align-baseline" onClick={onTurnOnPagination}>
                            Turn on pagination
                        </Button>{" "}
                        to fetch more {itemsName}
                    </>
                ) : (
                    <>Use a query to find more {itemsName}</>
                )}
            </span>
        </div>
    );
}
