import React from "react";
import { Carousel, CarouselItem, Nav } from "reactstrap";
import useIndexCleanup from "./useIndexCleanup";
import { LoadError } from "components/common/LoadError";
import { AboutViewHeading } from "components/common/AboutView";
import { FlexGrow } from "components/common/FlexGrow";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import IndexCleanupAboutView from "components/pages/database/indexes/cleanup/IndexCleanupAboutView";
import MergeIndexesNavItem from "components/pages/database/indexes/cleanup/navItems/MergeIndexesNavItem";
import RemoveSubindexesNavItem from "components/pages/database/indexes/cleanup/navItems/RemoveSubindexesNavItem";
import UnmergableIndexesNavItem from "components/pages/database/indexes/cleanup/navItems/UnmergableIndexesNavItem";
import RemoveUnusedIndexesNavItem from "components/pages/database/indexes/cleanup/navItems/RemoveUnusedIndexesNavItem";
import MergeSuggestionsErrorsNavItem from "components/pages/database/indexes/cleanup/navItems/MergeSuggestionsErrorsNavItem";
import MergeIndexesCard from "components/pages/database/indexes/cleanup/carouselCards/MergeIndexesCard";
import RemoveSubindexesCard from "components/pages/database/indexes/cleanup/carouselCards/RemoveSubindexesCard";
import RemoveUnusedIndexesCard from "components/pages/database/indexes/cleanup/carouselCards/RemoveUnusedIndexesCard";
import UnmergableIndexesCard from "components/pages/database/indexes/cleanup/carouselCards/UnmergableIndexesCard";
import MergeSuggestionsErrorsCarouselCard from "components/pages/database/indexes/cleanup/carouselCards/MergeSuggestionsErrorsCarouselCard";
import { LazyLoad } from "components/common/LazyLoad";

export function IndexCleanup() {
    const { asyncFetchStats, carousel, mergable, surpassing, unused, unmergable, errors } = useIndexCleanup();
    const hasIndexCleanup = useAppSelector(licenseSelectors.statusValue("HasIndexCleanup"));

    const isLoadActive = asyncFetchStats.status === "not-requested" || asyncFetchStats.status === "loading";

    if (hasIndexCleanup && asyncFetchStats.status === "error") {
        return <LoadError error="Unable to load index cleanup data" refresh={asyncFetchStats.execute} />;
    }

    return (
        <div className="content-margin gy-sm">
            <div className="flex-shrink-0 hstack gap-2 mb-4 align-items-start">
                <AboutViewHeading
                    icon="index-cleanup"
                    title="Index Cleanup"
                    licenseBadgeText={hasIndexCleanup ? null : "Professional +"}
                />
                <FlexGrow />
                <IndexCleanupAboutView />
            </div>

            <div className={hasIndexCleanup ? "" : "item-disabled pe-none"}>
                <LazyLoad active={isLoadActive}>
                    <Nav className="card-tabs gap-3 card-tabs">
                        <MergeIndexesNavItem carousel={carousel} mergable={mergable} />
                        <RemoveSubindexesNavItem carousel={carousel} surpassing={surpassing} />
                        <RemoveUnusedIndexesNavItem carousel={carousel} unused={unused} />
                        <UnmergableIndexesNavItem carousel={carousel} unmergable={unmergable} />
                        {errors.data.length > 0 && (
                            <MergeSuggestionsErrorsNavItem carousel={carousel} errors={errors} />
                        )}
                    </Nav>
                </LazyLoad>
                <LazyLoad active={isLoadActive}>
                    <Carousel
                        activeIndex={carousel.activeTab}
                        className="carousel-auto-height mt-3 mb-4"
                        style={{ height: carousel.carouselHeight }}
                        previous={() => null}
                        next={() => null}
                    >
                        <CarouselItem key="carousel-0" onEntering={() => carousel.setHeight(0)}>
                            <div ref={(el) => (carousel.carouselRefs.current[0] = el)}>
                                <MergeIndexesCard mergable={mergable} />
                            </div>
                        </CarouselItem>
                        <CarouselItem key="carousel-1" onEntering={() => carousel.setHeight(1)}>
                            <div ref={(el) => (carousel.carouselRefs.current[1] = el)}>
                                <RemoveSubindexesCard surpassing={surpassing} />
                            </div>
                        </CarouselItem>
                        <CarouselItem key="carousel-2" onEntering={() => carousel.setHeight(2)}>
                            <div ref={(el) => (carousel.carouselRefs.current[2] = el)}>
                                <RemoveUnusedIndexesCard unused={unused} />
                            </div>
                        </CarouselItem>
                        <CarouselItem key="carousel-3" onEntering={() => carousel.setHeight(3)}>
                            <div ref={(el) => (carousel.carouselRefs.current[3] = el)}>
                                <UnmergableIndexesCard unmergable={unmergable} />
                            </div>
                        </CarouselItem>
                        {errors.data.length > 0 && (
                            <CarouselItem key="carousel-4" onEntering={() => carousel.setHeight(4)}>
                                <div ref={(el) => (carousel.carouselRefs.current[4] = el)}>
                                    <MergeSuggestionsErrorsCarouselCard errors={errors.data} />
                                </div>
                            </CarouselItem>
                        )}
                    </Carousel>
                </LazyLoad>
            </div>
        </div>
    );
}
