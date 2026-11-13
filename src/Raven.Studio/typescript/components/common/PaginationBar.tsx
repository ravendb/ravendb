import { Icon } from "components/common/Icon";
import Select, { SelectOption } from "components/common/select/Select";
import { ChangeEvent, FocusEvent, ReactNode, useState } from "react";
import Button from "react-bootstrap/Button";
import Form from "react-bootstrap/Form";
import classNames from "classnames";
import "./PaginationBar.scss";

export interface PaginationBarState {
    page: number;
    totalPages: number;
    firstItemNumber: number;
    lastItemNumber: number;
    totalCount: number | null;
    onPageChange: (page: number) => void;
    pageSizeSelector?: PageSizeSelector;
}

interface PageSizeSelector {
    pageSize: number;
    options: number[];
    onChange: (pageSize: number) => void;
}

interface PaginationBarProps {
    pagination: PaginationBarState;
    className?: string;
}

export function PaginationBar({ pagination, className }: PaginationBarProps) {
    const { page, totalPages, pageSizeSelector } = pagination;

    return (
        <div
            className={classNames("pagination-bar d-flex align-items-center justify-content-between gap-3", className)}
        >
            <div className="d-flex align-items-center gap-3">
                <small className="text-muted text-nowrap">{formatItemsRange(pagination)}</small>
                {pageSizeSelector && (
                    <>
                        <div className="vr" />
                        <label className="d-flex align-items-center gap-2 mb-0 text-nowrap">
                            <small className="text-muted">Rows per page</small>
                            <Select
                                className="page-size-select"
                                aria-label="Rows per page"
                                options={pageSizeSelector.options.map(toPageSizeOption)}
                                value={toPageSizeOption(pageSizeSelector.pageSize)}
                                onChange={(option) => pageSizeSelector.onChange(option.value)}
                                isSearchable={false}
                                menuPlacement="top"
                            />
                        </label>
                    </>
                )}
            </div>
            <PageNavigation page={page} totalPages={totalPages} onPageChange={pagination.onPageChange} />
        </div>
    );
}

interface PageNavigationProps {
    page: number;
    totalPages: number;
    onPageChange: (page: number) => void;
    isSimple?: boolean;
    className?: string;
}

const MIN_PAGES_FOR_SIMPLE_FIRST_AND_LAST_BUTTONS = 5;

export function PageNavigation({ page, totalPages, onPageChange, isSimple, className }: PageNavigationProps) {
    const hasFirstAndLastButtons = !isSimple || totalPages >= MIN_PAGES_FOR_SIMPLE_FIRST_AND_LAST_BUTTONS;

    return (
        <div className={classNames("page-navigation d-flex align-items-center gap-1", className)}>
            {hasFirstAndLastButtons && (
                <PageButton title="First page" isDisabled={page <= 1} onClick={() => onPageChange(1)}>
                    <Icon icon="chevron-left" margin="m-0" />
                    <Icon icon="chevron-left" margin="m-0" className="double-chevron" />
                </PageButton>
            )}
            <PageButton title="Previous page" isDisabled={page <= 1} onClick={() => onPageChange(page - 1)}>
                <Icon icon="chevron-left" margin="m-0" />
            </PageButton>
            {isSimple ? (
                <small className="text-muted mx-1 text-nowrap">
                    Page {page.toLocaleString()} of {totalPages.toLocaleString()}
                </small>
            ) : (
                <>
                    <small className="text-muted ms-1">Page</small>
                    <PageInput page={page} totalPages={totalPages} onPageChange={onPageChange} />
                    <small className="text-muted me-1 text-nowrap">of {totalPages.toLocaleString()}</small>
                </>
            )}
            <PageButton title="Next page" isDisabled={page >= totalPages} onClick={() => onPageChange(page + 1)}>
                <Icon icon="chevron-right" margin="m-0" />
            </PageButton>
            {hasFirstAndLastButtons && (
                <PageButton title="Last page" isDisabled={page >= totalPages} onClick={() => onPageChange(totalPages)}>
                    <Icon icon="chevron-right" margin="m-0" />
                    <Icon icon="chevron-right" margin="m-0" className="double-chevron" />
                </PageButton>
            )}
        </div>
    );
}

interface PageButtonProps {
    title: string;
    isDisabled: boolean;
    onClick: () => void;
    children: ReactNode;
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
    const [prevPage, setPrevPage] = useState(page);

    if (page !== prevPage) {
        setPrevPage(page);
        setValue(String(page));
    }

    const handleChange = (event: ChangeEvent<HTMLInputElement>) => {
        setValue(event.target.value);

        const parsedPage = Number(event.target.value);
        const isValidPage = Number.isInteger(parsedPage) && parsedPage >= 1 && parsedPage <= totalPages;

        if (isValidPage && parsedPage !== page) {
            onPageChange(parsedPage);
        }
    };

    return (
        <Form.Control
            type="number"
            size="sm"
            className="page-input text-center"
            aria-label="Page"
            min={1}
            max={totalPages}
            style={{ width: `calc(${String(totalPages).length}ch + 2.5em)` }}
            value={value}
            onChange={handleChange}
            onFocus={(event: FocusEvent<HTMLInputElement>) => event.target.select()}
            onBlur={() => setValue(String(page))}
        />
    );
}

function formatItemsRange({ firstItemNumber, lastItemNumber, totalCount }: PaginationBarState): string {
    if (firstItemNumber === 0) {
        return "No rows";
    }

    const range = `${firstItemNumber.toLocaleString()}-${lastItemNumber.toLocaleString()}`;
    const total = totalCount ? ` of ${totalCount.toLocaleString()}` : "";

    return range + total;
}

function toPageSizeOption(pageSize: number): SelectOption<number> {
    return { value: pageSize, label: pageSize.toLocaleString() };
}
