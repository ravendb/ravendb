import React from "react";
import { Icon } from "components/common/Icon";
import { Badge, Button, Card, Carousel, CarouselItem, Col, Nav, NavItem, Row, Table } from "reactstrap";
import { RichPanel, RichPanelHeader } from "components/common/RichPanel";
import { Checkbox } from "components/common/Checkbox";
import moment from "moment";
import { EmptySet } from "components/common/EmptySet";
import classNames from "classnames";
import database from "models/resources/database";
import useIndexCleanup from "./useIndexCleanup";
import { useAppUrls } from "components/hooks/useAppUrls";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import AboutViewFloating, {
    AboutViewAnchored,
    AboutViewHeading,
    AccordionItemWrapper,
} from "components/common/AboutView";
import { FlexGrow } from "components/common/FlexGrow";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";

const mergeIndexesImg = require("Content/img/pages/indexCleanup/merge-indexes.svg");
const removeSubindexesImg = require("Content/img/pages/indexCleanup/remove-subindexes.svg");
const removeUnusedImg = require("Content/img/pages/indexCleanup/remove-unused.svg");
const unmergableIndexesImg = require("Content/img/pages/indexCleanup/unmergable-indexes.svg");

interface IndexCleanupProps {
    db: database;
}

export function IndexCleanup(props: IndexCleanupProps) {
    const { db } = props;

    const { asyncFetchStats, carousel, mergable, surpassing, unused, unmergable } = useIndexCleanup(db);
    const { appUrl } = useAppUrls();

    const isProfessionalOrAbove = useAppSelector(licenseSelectors.isProfessionalOrAbove());

    if (asyncFetchStats.status === "not-requested" || asyncFetchStats.status === "loading") {
        return <LoadingView />;
    }

    if (asyncFetchStats.status === "error") {
        return <LoadError error="Unable to load index cleanup data" refresh={asyncFetchStats.execute} />;
    }

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <div className="flex-shrink-0 hstack gap-2 mb-4 align-items-start">
                            <AboutViewHeading
                                icon="index-cleanup"
                                title="Index Cleanup"
                                badgeText={isProfessionalOrAbove ? null : "Professional +"}
                            />

                            <FlexGrow />
                            <AboutViewFloating defaultOpen={!isProfessionalOrAbove}>
                                <AccordionItemWrapper
                                    targetId="about"
                                    icon="about"
                                    color="info"
                                    heading="About this view"
                                    description="Get additional info on this feature"
                                >
                                    <p>
                                        Maintaining multiple indexes can lower performance. Every time data is inserted,
                                        updated, or deleted, the corresponding indexes need to be updated as well, which
                                        can lead to increased write latency.
                                    </p>
                                    <p className="mb-0">
                                        To counter these performance issues, RavenDB recommends a set of actions to
                                        optimize the number of indexes. Note that you need to update the index reference
                                        in your application.
                                    </p>
                                </AccordionItemWrapper>
                                <AboutViewAnchored
                                    className="mt-3"
                                    defaultOpen={isProfessionalOrAbove ? null : "licensing"}
                                >
                                    <AccordionLicenseNotIncluded
                                        targetId="licensing"
                                        featureName="Index Cleanup"
                                        featureIcon="index-cleanup"
                                        checkedLicenses={["Professional", "Enterprise"]}
                                        isLimited={!isProfessionalOrAbove}
                                    />
                                </AboutViewAnchored>
                            </AboutViewFloating>
                        </div>
                        <div className={isProfessionalOrAbove ? "" : "item-disabled pe-none"}>
                            <Nav className="card-tabs gap-3 card-tabs">
                                <NavItem>
                                    <Card
                                        className={classNames("p-3", "card-tab", { active: carousel.activeTab === 0 })}
                                        onClick={() => carousel.setActiveTab(0)}
                                    >
                                        <img src={mergeIndexesImg} alt="" />
                                        <Badge
                                            className="rounded-pill fs-5"
                                            color={mergable.data.length !== 0 ? "primary" : "secondary"}
                                        >
                                            {mergable.data.length}
                                        </Badge>
                                        <h4 className="text-center">
                                            Merge
                                            <br />
                                            indexes
                                        </h4>
                                    </Card>
                                </NavItem>
                                <NavItem>
                                    <Card
                                        className={classNames("p-3", "card-tab", { active: carousel.activeTab === 1 })}
                                        onClick={() => carousel.setActiveTab(1)}
                                    >
                                        <img src={removeSubindexesImg} alt="" />
                                        <Badge
                                            className="rounded-pill fs-5"
                                            color={surpassing.data.length !== 0 ? "primary" : "secondary"}
                                        >
                                            {surpassing.data.length}
                                        </Badge>
                                        <h4 className="text-center">
                                            Remove
                                            <br />
                                            sub-indexes
                                        </h4>
                                    </Card>
                                </NavItem>
                                <NavItem>
                                    <Card
                                        className={classNames("p-3", "card-tab", { active: carousel.activeTab === 2 })}
                                        onClick={() => carousel.setActiveTab(2)}
                                    >
                                        <img src={removeUnusedImg} alt="" />
                                        <Badge
                                            className="rounded-pill fs-5"
                                            color={unused.data.length !== 0 ? "primary" : "secondary"}
                                        >
                                            {unused.data.length}
                                        </Badge>
                                        <h4 className="text-center">
                                            Remove <br />
                                            unused indexes
                                        </h4>
                                    </Card>
                                </NavItem>
                                <NavItem>
                                    <Card
                                        className={classNames("p-3", "card-tab", { active: carousel.activeTab === 3 })}
                                        onClick={() => carousel.setActiveTab(3)}
                                    >
                                        <img src={unmergableIndexesImg} alt="" />
                                        <Badge
                                            className="rounded-pill fs-5"
                                            color={unmergable.data.length !== 0 ? "primary" : "secondary"}
                                        >
                                            {unmergable.data.length}
                                        </Badge>
                                        <h4 className="text-center">
                                            Unmergable
                                            <br />
                                            indexes
                                        </h4>
                                    </Card>
                                </NavItem>
                            </Nav>

                            <Carousel
                                activeIndex={carousel.activeTab}
                                className="carousel-auto-height mt-3 mb-4"
                                style={{ height: carousel.carouselHeight }}
                                previous={() => null}
                                next={() => null}
                            >
                                <CarouselItem key="carousel-0" onEntering={() => carousel.setHeight(0)}>
                                    <div ref={(el) => (carousel.carouselRefs.current[0] = el)}>
                                        <Card>
                                            <Card className="bg-faded-primary p-4 d-block">
                                                <div className="text-limit-width">
                                                    <h2>Merge indexes</h2>
                                                    Combining several indexes with similar purposes into a single index
                                                    can reduce the number of times that data needs to be scanned.
                                                    <br />
                                                    Once a <strong>NEW</strong> merged index definition is created, the
                                                    original indexes can be removed.
                                                </div>
                                            </Card>
                                            <div className="p-2">
                                                {mergable.data.length === 0 ? (
                                                    <EmptySet>No indexes to merge</EmptySet>
                                                ) : (
                                                    <>
                                                        <div className="mx-3">
                                                            <Table className="mb-1 table-inner-border">
                                                                <tbody>
                                                                    <tr>
                                                                        <td></td>
                                                                        <td width={300}>
                                                                            <div className="small-label">
                                                                                Last query time
                                                                            </div>
                                                                        </td>
                                                                        <td width={300}>
                                                                            <div className="small-label">
                                                                                Last indexing time
                                                                            </div>
                                                                        </td>
                                                                    </tr>
                                                                </tbody>
                                                            </Table>
                                                        </div>

                                                        {mergable.data.map((mergableGroup, groupKey) => (
                                                            <RichPanel
                                                                key={
                                                                    "mergeGroup-" +
                                                                    mergableGroup.mergedIndexDefinition.Name
                                                                }
                                                                hover
                                                            >
                                                                <RichPanelHeader className="px-3 py-2 flex-wrap flex-row gap-3">
                                                                    <div className="mt-1">
                                                                        <Button
                                                                            color="primary"
                                                                            size="sm"
                                                                            className="rounded-pill"
                                                                            onClick={() =>
                                                                                mergable.navigateToMergeSuggestion(
                                                                                    mergableGroup
                                                                                )
                                                                            }
                                                                        >
                                                                            <Icon icon="merge" />
                                                                            Review suggested merge
                                                                        </Button>
                                                                    </div>
                                                                    <div className="flex-grow-1">
                                                                        <Table className="m-0 table-inner-border">
                                                                            <tbody>
                                                                                {mergableGroup.toMerge.map(
                                                                                    (index, indexKey) => (
                                                                                        <tr
                                                                                            key={
                                                                                                "index-" +
                                                                                                groupKey +
                                                                                                "-" +
                                                                                                indexKey
                                                                                            }
                                                                                        >
                                                                                            <td>
                                                                                                <div>
                                                                                                    <a
                                                                                                        href={appUrl.forEditIndex(
                                                                                                            index.name,
                                                                                                            db
                                                                                                        )}
                                                                                                    >
                                                                                                        {index.name}{" "}
                                                                                                        <Icon
                                                                                                            icon="newtab"
                                                                                                            margin="ms-1"
                                                                                                        />
                                                                                                    </a>
                                                                                                </div>
                                                                                            </td>

                                                                                            <td width={300}>
                                                                                                <div>
                                                                                                    {formatDate(
                                                                                                        index.lastQueryTime
                                                                                                    )}
                                                                                                </div>
                                                                                            </td>
                                                                                            <td width={300}>
                                                                                                <div>
                                                                                                    {formatDate(
                                                                                                        index.lastIndexingTime
                                                                                                    )}
                                                                                                </div>
                                                                                            </td>
                                                                                        </tr>
                                                                                    )
                                                                                )}
                                                                            </tbody>
                                                                        </Table>
                                                                    </div>
                                                                </RichPanelHeader>
                                                            </RichPanel>
                                                        ))}
                                                    </>
                                                )}
                                            </div>
                                        </Card>
                                    </div>
                                </CarouselItem>
                                <CarouselItem key="carousel-1" onEntering={() => carousel.setHeight(1)}>
                                    <div ref={(el) => (carousel.carouselRefs.current[1] = el)}>
                                        <Card>
                                            <Card className="bg-faded-primary p-4">
                                                <div className="text-limit-width">
                                                    <h2>Remove sub-indexes</h2>
                                                    If an index is completely covered by another index (i.e., all its
                                                    fields are present in the larger index) maintaining it does not
                                                    provide any value and only adds unnecessary overhead. You can remove
                                                    the subset index without losing any query optimization benefits.
                                                </div>
                                            </Card>
                                            {surpassing.data.length === 0 ? (
                                                <EmptySet>No subset indexes</EmptySet>
                                            ) : (
                                                <div className="p-2">
                                                    <ButtonWithSpinner
                                                        color="primary"
                                                        className="mb-2 rounded-pill"
                                                        onClick={surpassing.deleteSelected}
                                                        isSpinning={surpassing.isDeleting}
                                                        disabled={surpassing.selected.length === 0}
                                                    >
                                                        <Icon icon="trash" />
                                                        Delete selected sub-indexes{" "}
                                                        <Badge color="faded-primary" className="rounded-pill ms-1">
                                                            {surpassing.selected.length}
                                                        </Badge>
                                                    </ButtonWithSpinner>

                                                    <RichPanel hover>
                                                        <RichPanelHeader className="px-3 py-2 d-block">
                                                            <Table responsive className="m-0 table-inner-border">
                                                                <thead>
                                                                    <tr>
                                                                        <td>
                                                                            <Checkbox
                                                                                size="lg"
                                                                                color="primary"
                                                                                selected={
                                                                                    surpassing.selectionState ===
                                                                                    "AllSelected"
                                                                                }
                                                                                indeterminate={
                                                                                    surpassing.selectionState ===
                                                                                    "SomeSelected"
                                                                                }
                                                                                toggleSelection={surpassing.toggleAll}
                                                                            />
                                                                        </td>
                                                                        <td className="align-middle">
                                                                            <div className="small-label">Sub-index</div>
                                                                        </td>
                                                                        <td width={50}></td>
                                                                        <td className="align-middle">
                                                                            <div className="small-label">
                                                                                Containing index
                                                                            </div>
                                                                        </td>
                                                                        <td className="align-middle">
                                                                            <div className="small-label">
                                                                                Last query time (sub-index)
                                                                            </div>
                                                                        </td>
                                                                        <td className="align-middle">
                                                                            <div className="small-label">
                                                                                Last indexing time (sub-index)
                                                                            </div>
                                                                        </td>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {surpassing.data.map((index) => (
                                                                        <tr key={"subindex-" + index.name}>
                                                                            <td>
                                                                                <Checkbox
                                                                                    size="lg"
                                                                                    selected={surpassing.selected.includes(
                                                                                        index.name
                                                                                    )}
                                                                                    toggleSelection={(x) =>
                                                                                        surpassing.toggle(
                                                                                            x.currentTarget.checked,
                                                                                            index.name
                                                                                        )
                                                                                    }
                                                                                />
                                                                            </td>
                                                                            <td>
                                                                                <div>
                                                                                    <a
                                                                                        href={appUrl.forEditIndex(
                                                                                            index.name,
                                                                                            db
                                                                                        )}
                                                                                    >
                                                                                        {index.name}{" "}
                                                                                        <Icon
                                                                                            icon="newtab"
                                                                                            margin="ms-1"
                                                                                        />
                                                                                    </a>
                                                                                </div>
                                                                            </td>
                                                                            <td>
                                                                                <div>⊇</div>
                                                                            </td>
                                                                            <td>
                                                                                <div>
                                                                                    <a
                                                                                        href={appUrl.forEditIndex(
                                                                                            index.containingIndexName,
                                                                                            db
                                                                                        )}
                                                                                    >
                                                                                        {index.containingIndexName}{" "}
                                                                                        <Icon
                                                                                            icon="newtab"
                                                                                            margin="ms-1"
                                                                                        />
                                                                                    </a>
                                                                                </div>
                                                                            </td>
                                                                            <td width={300}>
                                                                                <div>
                                                                                    {formatDate(index.lastQueryingTime)}
                                                                                </div>
                                                                            </td>
                                                                            <td width={300}>
                                                                                <div>
                                                                                    {formatDate(index.lastIndexingTime)}
                                                                                </div>
                                                                            </td>
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </Table>
                                                        </RichPanelHeader>
                                                    </RichPanel>
                                                </div>
                                            )}
                                        </Card>
                                    </div>
                                </CarouselItem>
                                <CarouselItem key="carousel-2" onEntering={() => carousel.setHeight(2)}>
                                    <div ref={(el) => (carousel.carouselRefs.current[2] = el)}>
                                        <Card>
                                            <Card className="bg-faded-primary p-4">
                                                <div className="text-limit-width">
                                                    <h2>Remove unused indexes</h2>
                                                    Unused indexes still consume resources. Indexes that have not been
                                                    queried for over a week are listed below. Review the list and
                                                    consider deleting any unnecessary indexes.
                                                </div>
                                            </Card>
                                            {unused.data.length === 0 ? (
                                                <EmptySet>No unused indexes</EmptySet>
                                            ) : (
                                                <div className="p-2">
                                                    <ButtonWithSpinner
                                                        color="primary"
                                                        className="mb-2 rounded-pill"
                                                        onClick={unused.deleteSelected}
                                                        isSpinning={unused.isDeleting}
                                                        disabled={unused.selected.length === 0}
                                                    >
                                                        <Icon icon="trash" />
                                                        Delete selected indexes
                                                        <Badge color="faded-primary" className="rounded-pill ms-1">
                                                            {unused.selected.length}
                                                        </Badge>
                                                    </ButtonWithSpinner>
                                                    <RichPanel hover>
                                                        <RichPanelHeader className="px-3 py-2 d-block">
                                                            <Table responsive className="m-0 table-inner-border">
                                                                <thead>
                                                                    <tr>
                                                                        <td>
                                                                            <Checkbox
                                                                                size="lg"
                                                                                color="primary"
                                                                                selected={
                                                                                    unused.selectionState ===
                                                                                    "AllSelected"
                                                                                }
                                                                                indeterminate={
                                                                                    unused.selectionState ===
                                                                                    "SomeSelected"
                                                                                }
                                                                                toggleSelection={unused.toggleAll}
                                                                            />
                                                                        </td>
                                                                        <td className="align-middle">
                                                                            <div className="small-label">
                                                                                Unused index
                                                                            </div>
                                                                        </td>

                                                                        <td className="align-middle">
                                                                            <div className="small-label">
                                                                                Last query time
                                                                            </div>
                                                                        </td>
                                                                        <td className="align-middle">
                                                                            <div className="small-label">
                                                                                Last indexing time
                                                                            </div>
                                                                        </td>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {unused.data.map((index) => (
                                                                        <tr key={"unusedIndex-" + index.name}>
                                                                            <td>
                                                                                <Checkbox
                                                                                    size="lg"
                                                                                    selected={unused.selected.includes(
                                                                                        index.name
                                                                                    )}
                                                                                    toggleSelection={(x) =>
                                                                                        unused.toggle(
                                                                                            x.currentTarget.checked,
                                                                                            index.name
                                                                                        )
                                                                                    }
                                                                                />
                                                                            </td>
                                                                            <td>
                                                                                <div>
                                                                                    <a
                                                                                        href={appUrl.forEditIndex(
                                                                                            index.name,
                                                                                            db
                                                                                        )}
                                                                                    >
                                                                                        {index.name}{" "}
                                                                                        <Icon
                                                                                            icon="newtab"
                                                                                            margin="ms-1"
                                                                                        />
                                                                                    </a>
                                                                                </div>
                                                                            </td>
                                                                            <td width={300}>
                                                                                <div>
                                                                                    {formatDate(index.lastQueryingTime)}
                                                                                </div>
                                                                            </td>
                                                                            <td width={300}>
                                                                                <div>
                                                                                    {formatDate(index.lastIndexingTime)}
                                                                                </div>
                                                                            </td>
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </Table>
                                                        </RichPanelHeader>
                                                    </RichPanel>
                                                </div>
                                            )}
                                        </Card>
                                    </div>
                                </CarouselItem>
                                <CarouselItem key="carousel-3" onEntering={() => carousel.setHeight(3)}>
                                    <div ref={(el) => (carousel.carouselRefs.current[3] = el)}>
                                        <Card>
                                            <Card className="bg-faded-primary p-4">
                                                <div className="text-limit-width">
                                                    <h2>Unmergable indexes</h2>
                                                    The following indexes cannot be merged. See the specific reason
                                                    explanation provided for each index.
                                                </div>
                                            </Card>

                                            {unmergable.data.length === 0 ? (
                                                <EmptySet>No unmergable indexes</EmptySet>
                                            ) : (
                                                <div className="p-2">
                                                    <RichPanel hover>
                                                        <RichPanelHeader className="px-3 py-2 d-block">
                                                            <Table responsive className="m-0 table-inner-border">
                                                                <thead>
                                                                    <tr>
                                                                        <td>
                                                                            <div className="small-label">
                                                                                Index name
                                                                            </div>
                                                                        </td>

                                                                        <td>
                                                                            <div className="small-label">
                                                                                Unmergable reason
                                                                            </div>
                                                                        </td>
                                                                    </tr>
                                                                </thead>
                                                                <tbody>
                                                                    {unmergable.data.map((index, indexKey) => (
                                                                        <tr key={"unmergable-" + indexKey}>
                                                                            <td>
                                                                                <div>
                                                                                    <a
                                                                                        href={appUrl.forEditIndex(
                                                                                            index.name,
                                                                                            db
                                                                                        )}
                                                                                    >
                                                                                        {index.name}
                                                                                        <Icon
                                                                                            icon="newtab"
                                                                                            margin="ms-1"
                                                                                        />
                                                                                    </a>
                                                                                </div>
                                                                            </td>
                                                                            <td>
                                                                                <div>{index.reason}</div>
                                                                            </td>
                                                                        </tr>
                                                                    ))}
                                                                </tbody>
                                                            </Table>
                                                        </RichPanelHeader>
                                                    </RichPanel>
                                                </div>
                                            )}
                                        </Card>
                                    </div>
                                </CarouselItem>
                            </Carousel>
                        </div>
                    </Col>
                </Row>
            </Col>
        </div>
    );
}

const formatDate = (date: Date) => {
    return (
        <>
            {moment.utc(date).local().fromNow()}{" "}
            <small className="text-muted">({moment.utc(date).format("MM/DD/YY, h:mma")})</small>
        </>
    );
};
