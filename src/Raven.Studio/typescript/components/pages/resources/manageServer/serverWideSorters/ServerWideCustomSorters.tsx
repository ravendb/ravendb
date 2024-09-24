import React from "react";
import { Button, Col, Row } from "reactstrap";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useAsync } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import ServerWideCustomSortersList from "./ServerWideCustomSortersList";
import FeatureNotAvailableInYourLicensePopover from "components/common/FeatureNotAvailableInYourLicensePopover";
import { useCustomSorters } from "components/common/customSorters/useCustomSorters";
import ServerWideCustomSortersInfoHub from "components/pages/resources/manageServer/serverWideSorters/ServerWideCustomSortersInfoHub";
import RichAlert from "components/common/RichAlert";

export default function ServerWideCustomSorters() {
    const { sorters, setSorters, addNewSorter, removeSorter, mapFromDto } = useCustomSorters();

    const hasServerWideCustomSorters = useAppSelector(licenseSelectors.statusValue("HasServerWideCustomSorters"));

    const { manageServerService } = useServices();

    const asyncGetSorters = useAsync(
        async () => {
            if (!hasServerWideCustomSorters) {
                return [];
            }
            return await manageServerService.getServerWideCustomSorters();
        },
        [],
        {
            onSuccess(result) {
                setSorters(mapFromDto(result));
            },
        }
    );

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading
                            title="Server-Wide Sorters"
                            icon="server-wide-custom-sorters"
                            licenseBadgeText={hasServerWideCustomSorters ? null : "Professional +"}
                        />
                        {sorters.length > 0 && (
                            <RichAlert variant="info">
                                To test server-wide sorters, go to the Custom Sorters view in a database
                            </RichAlert>
                        )}
                        <div id="newServerWideCustomSorter" className="w-fit-content mt-4">
                            <Button
                                color="primary"
                                className="mb-3"
                                onClick={addNewSorter}
                                disabled={!hasServerWideCustomSorters}
                            >
                                <Icon icon="plus" />
                                Add a server-wide custom sorter
                            </Button>
                        </div>
                        {!hasServerWideCustomSorters && (
                            <FeatureNotAvailableInYourLicensePopover target="newServerWideCustomSorter" />
                        )}
                        <div className={hasServerWideCustomSorters ? null : "item-disabled pe-none"}>
                            <HrHeader count={sorters.length}>Server-wide custom sorters</HrHeader>
                            <ServerWideCustomSortersList
                                sorters={sorters}
                                fetchStatus={asyncGetSorters.status}
                                reload={asyncGetSorters.execute}
                                remove={removeSorter}
                            />
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <ServerWideCustomSortersInfoHub />
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
