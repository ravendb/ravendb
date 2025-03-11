import React from "react";
import Pagination from "react-bootstrap/Pagination";
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
                    <Pagination.Item
                        key={i}
                        active={i === page}
                        href="#"
                        onClick={withPreventDefault(() => onPageChange(i))}
                        linkClassName="no-decor"
                    >
                        {i}
                    </Pagination.Item>
                );
            }
        }

        if (startRange > 1) {
            pages.unshift(<Pagination.Ellipsis href="#" linkClassName="no-decor" key="start-ellipsis" disabled />);
            pages.unshift(
                <Pagination.Item
                    href="#"
                    onClick={withPreventDefault(() => onPageChange(1))}
                    linkClassName="no-decor"
                    key="1"
                >
                    1
                </Pagination.Item>
            );
        }

        if (endRange < totalPages) {
            pages.push(<Pagination.Ellipsis key="end-ellipsis" href="#" linkClassName="no-decor" disabled />);
            pages.push(
                <Pagination.Item
                    href="#"
                    onClick={withPreventDefault(() => onPageChange(totalPages))}
                    linkClassName="no-decor"
                    key={totalPages}
                >
                    {totalPages}
                </Pagination.Item>
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
                <Pagination.Prev
                    href="#"
                    onClick={withPreventDefault(() => onPageChange(Math.max(1, page - 1)))}
                    linkClassName="no-decor nav-arrow-btn"
                    disabled={page === 1}
                >
                    <Icon icon="arrow-thin-left" margin="m-0" />
                </Pagination.Prev>
                {getPaginationItems()}
                <Pagination.Next
                    href="#"
                    onClick={withPreventDefault(() => onPageChange(Math.min(totalPages, page + 1)))}
                    linkClassName="no-decor nav-arrow-btn"
                    disabled={page === totalPages}
                >
                    <Icon icon="arrow-thin-right" margin="m-0" />
                </Pagination.Next>
            </div>
        </Pagination>
    );
}
