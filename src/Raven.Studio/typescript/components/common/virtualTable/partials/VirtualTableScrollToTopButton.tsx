import { Icon } from "components/common/Icon";
import { useEffect, useState } from "react";
import Button from "react-bootstrap/Button";

interface VirtualTableScrollToTopButtonProps {
    tableContainerRef: React.MutableRefObject<HTMLDivElement>;
}

export default function VirtualTableScrollToTopButton({ tableContainerRef }: VirtualTableScrollToTopButtonProps) {
    const [isScrolledFromTop, setIsScrolledFromTop] = useState(false);

    useEffect(() => {
        const element = tableContainerRef.current;
        if (!element) {
            return;
        }

        const updateIsScrolledFromTop = () => setIsScrolledFromTop(element.scrollTop > 0);

        updateIsScrolledFromTop();
        element.addEventListener("scroll", updateIsScrolledFromTop, { passive: true });

        return () => element.removeEventListener("scroll", updateIsScrolledFromTop);
    }, [tableContainerRef]);

    if (!isScrolledFromTop) {
        return null;
    }

    // instant, so a lazy table does not fetch the rows it would pass on the way up
    return (
        <Button
            variant="secondary"
            className="scroll-to-top rounded-pill floating-bar"
            title="Scroll to top"
            aria-label="Scroll to top"
            onClick={() => tableContainerRef.current?.scrollTo({ top: 0, behavior: "instant" })}
            size="sm"
        >
            <Icon icon="arrow-thin-top" margin="m-0" />
        </Button>
    );
}
