import { PropsWithChildren, useState } from "react";
import Button from "react-bootstrap/Button";
import { Icon } from "components/common/Icon";
import { AnimatePresence, motion } from "motion/react";
import { ClassNameProps } from "components/models/common";

export type SheetSlideDirection = 1 | -1;

export function useSheetSlideNavigation(initialIndex: number, itemsCount: number) {
    const [currentIndex, setCurrentIndex] = useState(initialIndex);
    const [direction, setDirection] = useState<SheetSlideDirection>(1);

    const hasPrevious = currentIndex > 0;
    const hasNext = currentIndex < itemsCount - 1;

    const navigate = (dir: SheetSlideDirection) => {
        setDirection(dir);
        setCurrentIndex((i) => i + dir);
    };

    return { currentIndex, direction, navigate, hasPrevious, hasNext };
}

const slideVariants = {
    enter: (d: number) => ({ x: `${d * 100}%` }),
    center: { x: 0 },
    exit: (d: number) => ({ x: `${d * -100}%` }),
};

interface SheetSlideTransitionProps extends Required<PropsWithChildren>, ClassNameProps {
    currentIndex: number;
    direction: SheetSlideDirection;
}

export function SheetSlideTransition({ currentIndex, direction, className, children }: SheetSlideTransitionProps) {
    return (
        <div style={{ overflow: "hidden", position: "relative" }}>
            <AnimatePresence mode="popLayout" custom={direction} initial={false}>
                <motion.div
                    key={currentIndex}
                    custom={direction}
                    variants={slideVariants}
                    initial="enter"
                    animate="center"
                    exit="exit"
                    transition={{ duration: 0.3, ease: "easeInOut" }}
                    className={className}
                >
                    {children}
                </motion.div>
            </AnimatePresence>
        </div>
    );
}

interface SheetNavigationButtonsProps {
    hasPrevious: boolean;
    hasNext: boolean;
    navigate: (dir: SheetSlideDirection) => void;
}

export function SheetNavigationButtons({ hasPrevious, hasNext, navigate }: SheetNavigationButtonsProps) {
    return (
        <div className="d-flex gap-2">
            <Button className="rounded-pill" variant="secondary" disabled={!hasPrevious} onClick={() => navigate(-1)}>
                <Icon icon="arrow-thin-left" />
                Previous
            </Button>
            <Button className="rounded-pill" variant="secondary" disabled={!hasNext} onClick={() => navigate(1)}>
                Next
                <Icon icon="arrow-thin-right" margin="ms-1" />
            </Button>
        </div>
    );
}
