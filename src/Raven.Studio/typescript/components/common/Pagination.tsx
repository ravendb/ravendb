import React from "react";
import { Pagination, PaginationItem, PaginationLink } from "reactstrap";
import "./Pagination.scss";
import { Icon } from "components/common/Icon";
import { withPreventDefault } from "components/utils/common";

interface CustomPaginationProps {
    page: number;
    totalPages: number;
    showOnSinglePage?: number;
    onPageChange: (page: number) => void;
}

export default function CustomPagination(props: CustomPaginationProps) {
    const { totalPages, onPageChange, showOnSinglePage, page } = props;

    const getPaginationItems = () => {
        const pages: React.ReactNode[] = [];
        let startRange = page - 1;
        let endRange = page + 1;

        if (startRange <= 2) {
            startRange = 1;
            endRange = 5;
        }

        if (endRange >= totalPages - 1) {
            startRange = totalPages - 4;
            endRange = totalPages;
        }

        for (let i = startRange; i <= endRange; i++) {
            if (i > 0 && i <= totalPages) {
                pages.push(
                    <PaginationItem active={i === page} key={i}>
                        <PaginationLink
                            href="#"
                            onClick={withPreventDefault(() => onPageChange(i))}
                            className="no-decor"
                        >
                            {i}
                        </PaginationLink>
                    </PaginationItem>
                );
            }
        }

        if (startRange > 1) {
            pages.unshift(
                <PaginationItem key="start-ellipsis" disabled>
                    <PaginationLink href="#" className="no-decor">
                        ...
                    </PaginationLink>
                </PaginationItem>
            );
            pages.unshift(
                <PaginationItem key="1">
                    <PaginationLink href="#" onClick={withPreventDefault(() => onPageChange(1))} className="no-decor">
                        1
                    </PaginationLink>
                </PaginationItem>
            );
        }

        if (endRange < totalPages) {
            pages.push(
                <PaginationItem key="end-ellipsis" disabled>
                    <PaginationLink href="#" className="no-decor">
                        ...
                    </PaginationLink>
                </PaginationItem>
            );
            pages.push(
                <PaginationItem key={totalPages}>
                    <PaginationLink
                        href="#"
                        onClick={withPreventDefault(() => onPageChange(totalPages))}
                        className="no-decor"
                    >
                        {totalPages}
                    </PaginationLink>
                </PaginationItem>
            );
        }

        return pages;
    };

    if (totalPages <= 1 && !showOnSinglePage) {
        return;
    }

    return (
        <Pagination size="sm">
            <div className="d-flex gap-1">
                <PaginationItem disabled={page === 1}>
                    <PaginationLink
                        href="#"
                        onClick={withPreventDefault(() => onPageChange(Math.max(1, page - 1)))}
                        className="no-decor nav-arrow-btn"
                    >
                        <Icon icon="arrow-thin-left" margin="m-0" />
                    </PaginationLink>
                </PaginationItem>
                {getPaginationItems()}
                <PaginationItem disabled={page === totalPages}>
                    <PaginationLink
                        next
                        href="#"
                        onClick={withPreventDefault(() => onPageChange(Math.min(totalPages, page + 1)))}
                        className="no-decor nav-arrow-btn"
                    >
                        <Icon icon="arrow-thin-right" margin="m-0" />
                    </PaginationLink>
                </PaginationItem>
            </div>
        </Pagination>
    );
}
