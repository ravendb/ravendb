import { Button, Modal, ModalBody, ModalFooter, UncontrolledPopover } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import React from "react";
import { aboutPageUrls } from "components/pages/resources/about/partials/common";

interface ChangelogModalProps {
    mode: "whatsNew" | "changeLog" | "hidden";
    onClose: () => void;
}

// TODO: review entire file

export function ChangeLogModal(props: ChangelogModalProps) {
    const { mode, onClose } = props;

    const canUpgrade = true; //TODO:

    return (
        <Modal
            isOpen={mode !== "hidden"}
            toggle={onClose}
            wrapClassName="bs5"
            centered
            size="lg"
            contentClassName="modal-border bulge-warning"
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="logs" color="warning" className="fs-1" margin="m-0" />
                </div>

                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={onClose} />
                </div>
                <div className="text-center lead">Changelog</div>
                <h1>{mode} TODO</h1>
                <div>
                    <h3>
                        <strong className="text-warning">NEW</strong> - 6.0.0 (60002) - 2023/10/02{" "}
                    </h3>
                    <div className="d-flex gap-3">
                        {/* TODO add messages if no downgrade or needs license upgrade*/}
                        <div className="well px-3 py-1 small rounded-pill" id="updateDowngradeInfo">
                            <Icon icon="check" color="success" /> Can downgrade
                        </div>
                        <UncontrolledPopover
                            trigger="hover"
                            className="bs5"
                            placement="top"
                            target="updateDowngradeInfo"
                        >
                            <div className="px-2 py-1">This update is safe to revert to current version</div>
                        </UncontrolledPopover>
                        <div className="well px-3 py-1 small rounded-pill" id="updateLicenseInfo">
                            {canUpgrade ? (
                                <>
                                    <Icon icon="check" color="success" /> License compatible{" "}
                                </>
                            ) : (
                                <>
                                    <Icon icon="license" color="warning" /> Requires License Upgrade{" "}
                                </>
                            )}
                        </div>
                        <UncontrolledPopover trigger="hover" className="bs5" placement="top" target="updateLicenseInfo">
                            <div className="px-2 py-1">
                                {canUpgrade ? (
                                    <>
                                        This update is compatible with your license. In order to upgrade to the latest
                                        version
                                    </>
                                ) : (
                                    <>LatestVersion your license must be updated</>
                                )}
                            </div>
                        </UncontrolledPopover>
                    </div>
                    <div className="mt-4 vstack gap-2">{sampleChangeLog}</div>
                </div>
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" outline onClick={onClose} className="rounded-pill px-3">
                    Close
                </Button>
                <FlexGrow />
                <Button color="primary" className="rounded-pill px-3" href={aboutPageUrls.updateInstructions}>
                    Update instructions <Icon icon="newtab" margin="m-0" />
                </Button>
            </ModalFooter>
        </Modal>
    );
}

const sampleChangeLog = //TODO: delete and use real one
    (
        <React.Fragment>
            <h3>Features</h3>
            <ul>
                <li>
                    <code>[Corax]</code> new search & indexing engine. More <a href="#">here</a>
                </li>
                <li>
                    <code>[Sharding]</code> added &apos;sharding&apos; feature. More <a href="#">here</a>
                </li>
                <li>
                    <code>[Queue Sinks]</code> added Kafka and RabbitMQ sink. More <a href="#">here</a>
                </li>
                <li>
                    <code>[Data Archival]</code> added &apos;data archival&apos; feature. More <a href="#">here</a>
                </li>
            </ul>
            <h3>Upgrading from previous versions</h3>
            <ul>
                <li>
                    4.x and 5.x licenses will not work with 6.x products and need to be converted via dedicated tool
                    available <a href="#">here</a>. After conversion license will continue working with previous
                    versions of the product, but can be also used with 6.x ones.
                </li>
                <li>
                    please refer to our <a href="#">Server migration guide</a> before proceeding with Server update and
                    check our list of Server breaking changes available <a href="#">here</a> and Client API breaking
                    changes available <a href="#">here</a>
                </li>
            </ul>
            <h3>Server</h3>
            <ul>
                <li>
                    <code>[Backups]</code> switched FTP backup implementation to use &apos;FluentFTP&apos;
                </li>
                <li>
                    <code>[Configuration]</code> changed default
                    &apos;Indexing.OrderByTicksAutomaticallyWhenDatesAreInvolved&apos; value to &apos;true&apos;
                </li>
                <li>
                    <code>[ETL]</code> OLAP ETL uses latest Parquet.Net package
                </li>
                <li>
                    <code>[ETL]</code> removed load error tolerance
                </li>
                <li>
                    <code>[Graph API]</code> removed support
                </li>
                <li>
                    <code>[Indexes]</code> new auto indexes will detect DateOnly and TimeOnly automatically
                </li>
                <li>
                    <code>[Indexes]</code> added the ability to &apos;test index&apos;. More here
                </li>
                <li>
                    <code>[JavaScript]</code> updated Jint to newest version
                </li>
                <li>
                    <code>[Monitoring]</code> added OIDs to track certificate expiration and usage
                </li>
                <li>
                    <code>[Querying]</code> when two boolean queries are merged in Lucene, boosting should be taken into
                    account properly to avoid merging queries with different boost value
                </li>
                <li>
                    <code>[Voron]</code> performance improvements
                </li>
            </ul>
            <h3>Client API</h3>
            <ul>
                <li>
                    [Compare Exchange] added support for creating an array as a value in
                    &apos;PutCompareExchangeValueOperation&apos;
                </li>
                <li>
                    [Compare Exchange] compare exchange includes should not override already tracked compare exchange
                    values in session to match behavior of regular entities
                </li>
                <li>[Conventions] HttpVersion was switched to 2.0</li>
                <li>
                    [Conventions] removed &apos;UseCompression&apos; and introduced &apos;UseHttpCompression&apos; and
                    &apos;UseHttpDecompression&apos;
                </li>
                <li>
                    [Conventions] introduced &apos;DisposeCertificate&apos; with default value set to &apos;true&apos;
                    to help users mitigate the X509Certificate2 leak. More info here
                </li>
                <li>
                    [Database] introduced &apos;DatabaseRecordBuilder&apos; for more fluent database record creation
                </li>
                <li>[Facets] removed FacetOptions from RangeFacets</li>
                <li>[Graph API] removed support</li>
                <li>[Patching] JSON Patch will use conventions when serializing operations</li>
                <li>[Session] private fields no longer will be picked when projecting from type</li>
                <li>
                    [Session] taking into account &apos;PropertyNameConverter&apos; when querying and determining field
                    names
                </li>
                <li>
                    [Session] when a document has an embedded object with &apos;Id&apos; property we will detect that
                    this is not root object to avoid generating &apos;id(doc)&apos; method there for projection
                </li>
                <li>[Session] no tracking session will throw if any includes are used</li>
                <li>removed obsoletes and marked a lot of types as sealed and internal</li>
                <li>changed a lot of count and paging related properties and fields from int32 to int64</li>
            </ul>
            <h3>Studio</h3>
            <ul>
                <li>[Dashboard] removed Server dashboard</li>
                <li>[Design] refreshed L&F</li>
            </ul>
            <h3>Test Driver</h3>
            <ul>
                <li>added &apos;PreConfigureDatabase&apos; method</li>
            </ul>
            <h3>Other</h3>
            <ul>
                <li>[Containers] docker will use non-root user. More info and migration guide here</li>
            </ul>
        </React.Fragment>
    );
