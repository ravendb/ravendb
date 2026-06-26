import { useEffect, useRef, useState } from "react";

interface Category {
    categoryName: string;
}

const getCategoryId = (categoryName: string) =>
    `ongoing-task-category-${categoryName.replace(/[^a-zA-Z0-9]/g, "-").toLowerCase()}`;

export function useScrollSpy(allCategories: Category[]) {
    const contentRef = useRef<HTMLDivElement>(null);
    const isScrollingRef = useRef(false);
    const scrollTimerRef = useRef<ReturnType<typeof setTimeout> | undefined>(undefined);

    const [activeCategory, setActiveCategory] = useState<string | null>(
        allCategories.length > 0 ? allCategories[0].categoryName : null
    );

    const scrollToCategory = (categoryName: string) => {
        const el = document.getElementById(getCategoryId(categoryName));
        if (el) {
            isScrollingRef.current = true;
            el.scrollIntoView({ behavior: "smooth", block: "start" });
        }
        setActiveCategory(categoryName);
    };

    useEffect(() => {
        const container = contentRef.current;
        if (!container || allCategories.length === 0) {
            return;
        }

        const handleScroll = () => {
            if (isScrollingRef.current) {
                // Debounce: clear the flag 150ms after the last scroll tick from the animation
                clearTimeout(scrollTimerRef.current);
                scrollTimerRef.current = setTimeout(() => {
                    isScrollingRef.current = false;
                }, 150);
                return;
            }

            const maxScrollTop = container.scrollHeight - container.clientHeight;
            const scrollProgress = maxScrollTop > 0 ? container.scrollTop / maxScrollTop : 0;
            const triggerFraction = 0.15 + scrollProgress * 0.7;
            const containerTop = container.getBoundingClientRect().top;
            const triggerY = containerTop + container.clientHeight * triggerFraction;

            let active = allCategories[0].categoryName;
            for (const category of allCategories) {
                const el = document.getElementById(getCategoryId(category.categoryName));
                if (el && el.getBoundingClientRect().top <= triggerY) {
                    active = category.categoryName;
                }
            }

            setActiveCategory(active);
        };

        container.addEventListener("scroll", handleScroll, { passive: true });

        return () => {
            container.removeEventListener("scroll", handleScroll);
            clearTimeout(scrollTimerRef.current);
        };
    }, [allCategories]);

    return { contentRef, activeCategory, setActiveCategory, scrollToCategory };
}
