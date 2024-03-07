import React, { useState } from "react";
import { Pagination, PaginationItem, PaginationLink } from "reactstrap";
import "./Pagination.scss";
import { Icon } from "components/common/Icon";

interface CustomPaginationProps {
    totalPages: number;
    onPageChange?: (page: number) => void;
}

export default function CustomPagination(props: CustomPaginationProps) {
    const { totalPages, onPageChange } = props;
    const [currentPage, setCurrentPage] = useState(1);

    const handlePageClick = (page: number) => {
        setCurrentPage(page);
        onPageChange?.(page);
    };

    const getPaginationItems = () => {
        const pages = [];
        let startRange = currentPage - 1;
        let endRange = currentPage + 1;

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
                    <PaginationItem active={i === currentPage} key={i}>
                        <PaginationLink href="#" onClick={() => handlePageClick(i)} className="no-decor">
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
                    <PaginationLink href="#" onClick={() => handlePageClick(1)} className="no-decor">
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
                    <PaginationLink href="#" onClick={() => handlePageClick(totalPages)} className="no-decor">
                        {totalPages}
                    </PaginationLink>
                </PaginationItem>
            );
        }

        return pages;
    };

    return (
        <Pagination size="sm">
            <div className="d-flex gap-1">
                <PaginationItem disabled={currentPage === 1}>
                    <PaginationLink
                        href="#"
                        onClick={() => handlePageClick(Math.max(1, currentPage - 1))}
                        className="no-decor nav-arrow-btn"
                    >
                        <Icon icon="arrow-thin-left" margin="m-0" />
                    </PaginationLink>
                </PaginationItem>
                {getPaginationItems()}
                <PaginationItem disabled={currentPage === totalPages}>
                    <PaginationLink
                        next
                        href="#"
                        onClick={() => handlePageClick(Math.min(totalPages, currentPage + 1))}
                        className="no-decor nav-arrow-btn"
                    >
                        <Icon icon="arrow-thin-right" margin="m-0" />
                    </PaginationLink>
                </PaginationItem>
            </div>
        </Pagination>
    );
}
