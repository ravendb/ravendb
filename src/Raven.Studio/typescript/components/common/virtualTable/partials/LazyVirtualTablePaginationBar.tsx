import { Icon } from "components/common/Icon";
import Select, { SelectOption } from "components/common/select/Select";
import { KeyboardEvent, useEffect, useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import "./LazyVirtualTablePaginationBar.scss";

export interface LazyVirtualTablePagination {
    page: number;
    totalPages: number;
    firstRowNumber: number;
    lastRowNumber: number;
    totalCount: number | null;
    pageSize: number;
    pageSizeOptions: number[];
    onPageChange: (page: number) => void;
    onPageSizeChange: (pageSize: number) => void;
    turnOff: (() => void) | null;
}

interface LazyVirtualTablePaginationBarProps {
    pagination: LazyVirtualTablePagination;
}

export default function LazyVirtualTablePaginationBar({ pagination }: LazyVirtualTablePaginationBarProps) {
    const { page, totalPages, firstRowNumber, lastRowNumber, totalCount, pageSize, pageSizeOptions } = pagination;

    const pageSizeSelectOptions: SelectOption<number>[] = pageSizeOptions.map((x) => ({
        value: x,
        label: x.toLocaleString(),
    }));
    const selectedPageSizeOption = pageSizeSelectOptions.find((x) => x.value === pageSize) ?? {
        value: pageSize,
        label: pageSize.toLocaleString(),
    };

    const rowsRange =
        firstRowNumber === 0 ? "No rows" : `${firstRowNumber.toLocaleString()}-${lastRowNumber.toLocaleString()}`;

    return (
        <div className="lazy-virtual-table-pagination d-flex align-items-center justify-content-between gap-3 pt-1">
            <div className="d-flex align-items-center gap-3">
                <small className="text-muted text-nowrap">
                    {rowsRange}
                    {totalCount ? ` of ${totalCount.toLocaleString()}` : ""}
                </small>
                <div className="vr" />
                <label className="d-flex align-items-center gap-2 mb-0 text-nowrap">
                    <small className="text-muted">Rows per page</small>
                    <Select
                        className="page-size-select"
                        aria-label="Rows per page"
                        options={pageSizeSelectOptions}
                        value={selectedPageSizeOption}
                        onChange={(option) => pagination.onPageSizeChange(option.value)}
                        isSearchable={false}
                        menuPlacement="top"
                    />
                </label>
            </div>
            <div className="d-flex align-items-center gap-3">
                <div className="d-flex align-items-center gap-1">
                    <PageButton title="First page" isDisabled={page <= 1} onClick={() => pagination.onPageChange(1)}>
                        <Icon icon="chevron-left" margin="m-0" />
                        <Icon icon="chevron-left" margin="m-0" className="double-chevron" />
                    </PageButton>
                    <PageButton
                        title="Previous page"
                        isDisabled={page <= 1}
                        onClick={() => pagination.onPageChange(page - 1)}
                    >
                        <Icon icon="chevron-left" margin="m-0" />
                    </PageButton>
                    <small className="text-muted ms-1">Page</small>
                    <PageInput page={page} totalPages={totalPages} onPageChange={pagination.onPageChange} />
                    <small className="text-muted me-1 text-nowrap">of {totalPages.toLocaleString()}</small>
                    <PageButton
                        title="Next page"
                        isDisabled={page >= totalPages}
                        onClick={() => pagination.onPageChange(page + 1)}
                    >
                        <Icon icon="chevron-right" margin="m-0" />
                    </PageButton>
                    <PageButton
                        title="Last page"
                        isDisabled={page >= totalPages}
                        onClick={() => pagination.onPageChange(totalPages)}
                    >
                        <Icon icon="chevron-right" margin="m-0" />
                        <Icon icon="chevron-right" margin="m-0" className="double-chevron" />
                    </PageButton>
                </div>
                {pagination.turnOff && (
                    <>
                        <div className="vr" />
                        <Button variant="link" size="sm" className="p-0 text-nowrap" onClick={pagination.turnOff}>
                            Turn off pagination
                        </Button>
                    </>
                )}
            </div>
        </div>
    );
}

interface PageButtonProps {
    title: string;
    isDisabled: boolean;
    onClick: () => void;
    children: React.ReactNode;
}

function PageButton({ title, isDisabled, onClick, children }: PageButtonProps) {
    return (
        <Button
            variant="link"
            size="sm"
            className="page-button d-flex align-items-center p-1"
            title={title}
            aria-label={title}
            disabled={isDisabled}
            onClick={onClick}
        >
            {children}
        </Button>
    );
}

interface PageInputProps {
    page: number;
    totalPages: number;
    onPageChange: (page: number) => void;
}

function PageInput({ page, totalPages, onPageChange }: PageInputProps) {
    const [value, setValue] = useState(String(page));

    useEffect(() => {
        setValue(String(page));
    }, [page]);

    const commit = () => {
        const parsedPage = parseInt(value, 10);

        if (Number.isNaN(parsedPage)) {
            setValue(String(page));
            return;
        }

        const nextPage = Math.min(Math.max(1, parsedPage), totalPages);
        setValue(String(nextPage));

        if (nextPage !== page) {
            onPageChange(nextPage);
        }
    };

    const handleKeyDown = (event: KeyboardEvent<HTMLInputElement>) => {
        if (event.key === "Enter") {
            commit();
        }
    };

    return (
        <Form.Control
            size="sm"
            className="page-input text-center"
            aria-label="Page"
            inputMode="numeric"
            value={value}
            onChange={(event) => setValue(event.target.value)}
            onBlur={commit}
            onKeyDown={handleKeyDown}
        />
    );
}
