import { ChevronLeft, ChevronRight } from "lucide-react";
import { Button } from "@/components/shadcn/ui/button";
import { Text } from "@/components/typography";

interface TablePaginationProps {
    pageIndex: number;
    pageSize: number;
    totalCount: number;
    onPageIndexChange: (pageIndex: number) => void;
}

export function TablePagination({ pageIndex, pageSize, totalCount, onPageIndexChange }: TablePaginationProps) {
    const pageCount = Math.max(1, Math.ceil(totalCount / pageSize));
    const rangeStart = totalCount === 0 ? 0 : pageIndex * pageSize + 1;
    const rangeEnd = Math.min(totalCount, (pageIndex + 1) * pageSize);

    return (
        <div className="flex items-center justify-between gap-3">
            <Text variant="muted" as="span" className="tabular-nums">
                {rangeStart}&ndash;{rangeEnd} of {totalCount}
            </Text>
            <div className="flex items-center gap-3">
                <Text variant="muted" as="span" className="tabular-nums">
                    Page {pageIndex + 1} of {pageCount}
                </Text>
                <div className="flex items-center gap-1">
                    <Button
                        variant="outline"
                        size="icon-sm"
                        aria-label="Previous page"
                        disabled={pageIndex === 0}
                        onClick={() => onPageIndexChange(pageIndex - 1)}
                    >
                        <ChevronLeft />
                    </Button>
                    <Button
                        variant="outline"
                        size="icon-sm"
                        aria-label="Next page"
                        disabled={pageIndex >= pageCount - 1}
                        onClick={() => onPageIndexChange(pageIndex + 1)}
                    >
                        <ChevronRight />
                    </Button>
                </div>
            </div>
        </div>
    );
}
