import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";
import { AboutViewHeading } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { HrHeader } from "components/common/HrHeader";
import { useServices } from "components/hooks/useServices";
import { useAsync } from "react-async-hook";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import { useCustomAnalyzers } from "components/common/customAnalyzers/useCustomAnalyzers";
import ServerWideCustomAnalyzersList from "components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzersList";
import ServerWideCustomAnalyzersInfoHub from "components/pages/resources/manageServer/serverWideAnalyzers/ServerWideCustomAnalyzersInfoHub";
import Button from "react-bootstrap/Button";
import { ConditionalPopover } from "components/common/ConditionalPopover";

export default function ServerWideCustomAnalyzers() {
    const { analyzers, setAnalyzers, addNewAnalyzer, removeAnalyzer, mapFromDto } = useCustomAnalyzers();

    const hasServerWideCustomAnalyzers = useAppSelector(licenseSelectors.statusValue("HasServerWideAnalyzers"));

    const { manageServerService } = useServices();

    const asyncGetAnalyzers = useAsync(
        async () => {
            if (!hasServerWideCustomAnalyzers) {
                return [];
            }
            return await manageServerService.getServerWideCustomAnalyzers();
        },
        [],
        {
            onSuccess(result) {
                setAnalyzers(mapFromDto(result));
            },
        }
    );

    return (
        <div className="content-margin">
            <Col xxl={12}>
                <Row className="gy-sm">
                    <Col>
                        <AboutViewHeading
                            title="Server-Wide Analyzers"
                            icon="server-wide-custom-analyzers"
                            licenseBadgeText={hasServerWideCustomAnalyzers ? null : "Professional +"}
                        />
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasServerWideCustomAnalyzers,
                                message: <FeatureNotAvailableInYourLicensePopoverBody />,
                            }}
                        >
                            <Button
                                variant="primary"
                                className="mb-3 mt-4"
                                onClick={addNewAnalyzer}
                                disabled={!hasServerWideCustomAnalyzers}
                            >
                                <Icon icon="plus" /> Add a server-wide custom analyzer
                            </Button>
                        </ConditionalPopover>

                        <div className={hasServerWideCustomAnalyzers ? null : "item-disabled pe-none"}>
                            <HrHeader count={analyzers.length}>Server-wide custom analyzers</HrHeader>
                            <ServerWideCustomAnalyzersList
                                analyzers={analyzers}
                                fetchStatus={asyncGetAnalyzers.status}
                                reload={asyncGetAnalyzers.execute}
                                remove={removeAnalyzer}
                            />
                        </div>
                    </Col>
                    <Col sm={12} lg={4}>
                        <ServerWideCustomAnalyzersInfoHub />
                    </Col>
                </Row>
            </Col>
        </div>
    );
}
