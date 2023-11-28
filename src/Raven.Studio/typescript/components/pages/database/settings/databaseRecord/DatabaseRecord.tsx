import React, { useState } from "react";
import { Alert, Button, Card, CardBody, Col, InputGroup, Row } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { Icon } from "components/common/Icon";
import { todo } from "common/developmentHelper";
import { useRavenLink } from "components/hooks/useRavenLink";
import { FormAceEditor, FormInput, FormSwitch } from "components/common/Form";
import { useForm } from "react-hook-form";
import { NonShardedViewProps } from "components/models/common";
import useConfirm from "components/common/ConfirmDialog";
import classNames from "classnames";
import { useAccessManager } from "hooks/useAccessManager";
import FeatureNotAvailable from "components/common/FeatureNotAvailable";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSlice";

todo("Feature", "Damian", "Add missing logic");
todo("Feature", "Damian", "Verify access levels");
todo("Feature", "Damian", "Connect to Studio");
todo("Feature", "Damian", "Remove legacy code");
todo("Other", "Danielle", "Add Info Hub text");

export default function DatabaseRecord({ db }: NonShardedViewProps) {
    const databaseRecordDocsLink = useRavenLink({ hash: "QRCNKH" });
    const { control } = useForm<null>({});

    const confirm = useConfirm();

    const [isEditMode, setEditMode] = useState(false);

    const isOperatorOrAbove =
        useAppSelector(accessManagerSelectors.effectiveDatabaseAccessLevel(db.name)) === "DatabaseAdmin";

    const isForbidden = !isOperatorOrAbove;

    const toggleEditMode = async () => {
        const options = {
            icon: "database-record",
            actionColor: "warning",
            title: "You're about to enter the edit mode",
            message: <EditModeRiskAlert />,
            confirmText: "I understand the risk and want to proceed",
        };

        const confirmed = await confirm(options);

        if (confirmed) {
            setEditMode(!isEditMode);
        }
    };

    const saveDatabaseRecord = async () => {
        const options = {
            icon: "save",
            actionColor: "warning",
            title: "Do you want to save changes?",
            confirmText: "Yes, save changes",
            confirmIcon: "save",
        };

        const confirmed = await confirm(options);

        if (confirmed) {
            setEditMode(false);
        }
    };

    const discardDatabaseRecord = async () => {
        const options = {
            icon: "database-record",
            actionColor: "primary",
            title: "Do you want to discard changes?",
            confirmText: "Yes, discard changes",
        };

        const confirmed = await confirm(options);

        if (confirmed) {
            setEditMode(false);
        }
    };

    const [isHideEmptyValues, setHideEmptyValues] = useState(false);

    const toggleEmptyValuesVisibility = () => {
        setHideEmptyValues(!isHideEmptyValues);
    };

    const [isRecordCollapsed, setRecordCollapsed] = useState(false);

    const toggleRecordCollapsed = () => {
        setRecordCollapsed(!isRecordCollapsed);
    };

    return (
        <>
            <div className="content-margin">
                {isOperatorOrAbove && (
                    <Col xxl={12}>
                        <Row className="gy-sm">
                            <Col>
                                <AboutViewHeading title="Database Record" icon="database-record" />
                                <div
                                    id="editMode"
                                    className={classNames(
                                        "d-flex gap-3 flex-wrap mb-3",
                                        !isEditMode && "justify-content-between"
                                    )}
                                >
                                    {isEditMode ? (
                                        <>
                                            <Button color="primary" onClick={saveDatabaseRecord}>
                                                Save
                                            </Button>
                                            <Button color="secondary" onClick={discardDatabaseRecord}>
                                                Cancel
                                            </Button>
                                        </>
                                    ) : (
                                        <>
                                            <Button color="primary">
                                                <Icon icon="refresh" />
                                                Refresh
                                            </Button>
                                            <Button color="secondary" onClick={toggleEditMode}>
                                                <Icon icon="edit" />
                                                Edit record
                                            </Button>
                                        </>
                                    )}
                                </div>
                                <Card>
                                    <CardBody className="d-flex flex-center flex-column flex-wrap gap-4">
                                        <InputGroup className="gap-1 flex-wrap flex-column">
                                            <div className="d-flex flex-wrap justify-content-between">
                                                <FormSwitch
                                                    control={control}
                                                    name="hideEmptyValues"
                                                    color="secondary"
                                                    className="mb-0"
                                                    onChange={toggleEmptyValuesVisibility}
                                                >
                                                    {isHideEmptyValues ? "Show" : "Hide"} empty values
                                                </FormSwitch>
                                                <Button
                                                    color="link"
                                                    size="xs"
                                                    className="p-0"
                                                    onClick={toggleRecordCollapsed}
                                                >
                                                    {isRecordCollapsed ? (
                                                        <span>
                                                            <Icon icon="expand-vertical" /> Expand record
                                                        </span>
                                                    ) : (
                                                        <span>
                                                            <Icon icon="collapse-vertical" /> Collapse record
                                                        </span>
                                                    )}
                                                </Button>
                                            </div>
                                            <FormAceEditor
                                                name="databaseRecord"
                                                control={control}
                                                mode="json"
                                                height="600px"
                                                defaultValue={databaseSample}
                                                readOnly={!isEditMode}
                                            />
                                        </InputGroup>
                                    </CardBody>
                                </Card>
                            </Col>
                            <Col sm={12} lg={4}>
                                <AboutViewAnchored>
                                    <AccordionItemWrapper
                                        targetId="1"
                                        icon="about"
                                        color="info"
                                        description="Get additional info on this feature"
                                        heading="About this view"
                                    >
                                        <p>Text for Database Record</p>
                                        <hr />
                                        <div className="small-label mb-2">useful links</div>
                                        <a href={databaseRecordDocsLink} target="_blank">
                                            <Icon icon="newtab" /> Docs - Database Record
                                        </a>
                                    </AccordionItemWrapper>
                                </AboutViewAnchored>
                            </Col>
                        </Row>
                    </Col>
                )}
                {isForbidden && (
                    <Col xxl={12}>
                        <FeatureNotAvailable badgeText="Insufficient access">
                            You are not authorized to view this page
                        </FeatureNotAvailable>
                    </Col>
                )}
            </div>
        </>
    );
}

function EditModeRiskAlert() {
    return (
        <Alert color="warning">
            <Icon icon="warning" />
            Tampering with the Database Record may result in unwanted behavior including loss of the database along with
            all its data
        </Alert>
    );
}

const databaseSample = `{
    "DatabaseName": "test",
    "Disabled": false,
    "Encrypted": false,
    "EtagForBackup": 38,
    "DeletionInProgress": {},
    "RollingIndexes": {},
    "DatabaseState": "Normal",
    "LockMode": "Unlock",
    "Topology": {
        "Members": [
            "A"
        ],
        "Promotables": [],
        "Rehabs": [],
        "PredefinedMentors": {},
        "DemotionReasons": {},
        "PromotablesStatus": {},
        "Stamp": {
            "Index": 33,
            "Term": 5,
            "LeadersTicks": -2
        },
        "DynamicNodesDistribution": false,
        "ReplicationFactor": 1,
        "PriorityOrder": [],
        "NodesModifiedAt": "2023-11-28T10:35:28.7272290Z",
        "DatabaseTopologyIdBase64": "ua5wvIwqU0yrPTyzsU0hkA",
        "ClusterTransactionIdBase64": "z39F5G9btUaWtpgb/trjKQ"
    },
    "Sharding": null,
    "ConflictSolverConfig": null,
    "DocumentsCompression": null,
    "Sorters": {},
    "Analyzers": {},
    "Indexes": {
        "Product/Search": {
            "ClusterState": {
                "LastIndex": 36,
                "LastStateIndex": 0,
                "LastRollingDeploymentIndex": 0
            },
            "LockMode": "Unlock",
            "AdditionalSources": {},
            "CompoundFields": [],
            "AdditionalAssemblies": [],
            "Maps": [
                "from p in docs.Products\\r\\nselect new\\r\\n{\\r\\n    p.Name,\\r\\n    p.Category,\\r\\n    p.Supplier,\\r\\n    p.PricePerUnit\\r\\n}"
            ],
            "Reduce": null,
            "Fields": {
                "Name": {
                    "Storage": null,
                    "Indexing": "Search",
                    "TermVector": "Yes",
                    "Spatial": null,
                    "Analyzer": null,
                    "Suggestions": true
                }
            },
            "Configuration": {},
            "SourceType": "Documents",
            "ArchivedDataProcessingBehavior": null,
            "Type": "Map",
            "OutputReduceToCollection": null,
            "ReduceOutputIndex": null,
            "PatternForOutputReduceToCollectionReferences": null,
            "PatternReferencesCollectionName": null,
            "DeploymentMode": null,
            "Name": "Product/Search",
            "Priority": "Normal",
            "State": null
        },
        "Orders/ByShipment/Location": {
            "ClusterState": {
                "LastIndex": 36,
                "LastStateIndex": 0,
                "LastRollingDeploymentIndex": 0
            },
            "LockMode": "Unlock",
            "AdditionalSources": {},
            "CompoundFields": [],
            "AdditionalAssemblies": [],
            "Maps": [
                "from order in docs.Orders\\r\\nwhere order.ShipTo.Location != null\\r\\nselect new\\r\\n{\\r\\n    order.Employee,\\r\\n    order.Company,\\r\\n    ShipmentLocation = CreateSpatialField(order.ShipTo.Location.Latitude, order.ShipTo.Location.Longitude)\\r\\n}"
            ],
            "Reduce": null,
            "Fields": {
                "ShipmentLocation": {
                    "Storage": null,
                    "Indexing": null,
                    "TermVector": null,
                    "Spatial": {
                        "Type": "Geography",
                        "Strategy": "GeohashPrefixTree",
                        "MaxTreeLevel": 9,
                        "MinX": -180,
                        "MaxX": 180,
                        "MinY": -90,
                        "MaxY": 90,
                        "Units": "Kilometers"
                    },
                    "Analyzer": null,
                    "Suggestions": null
                }
            },
            "Configuration": {},
            "SourceType": "Documents",
            "ArchivedDataProcessingBehavior": null,
            "Type": "Map",
            "OutputReduceToCollection": null,
            "ReduceOutputIndex": null,
            "PatternForOutputReduceToCollectionReferences": null,
            "PatternReferencesCollectionName": null,
            "DeploymentMode": null,
            "Name": "Orders/ByShipment/Location",
            "Priority": "Normal",
            "State": null
        },
        "Orders/ByCompany": {
            "ClusterState": {
                "LastIndex": 38,
                "LastStateIndex": 0,
                "LastRollingDeploymentIndex": 0
            },
            "LockMode": "Unlock",
            "AdditionalSources": {},
            "CompoundFields": [],
            "AdditionalAssemblies": [],
            "Maps": [
                "from order in docs.Orders\\r\\nselect new\\r\\n{\\r\\n    order.Company,\\r\\n    Count = 1,\\r\\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\\r\\n}"
            ],
            "Reduce": "from result in results\\r\\ngroup result by result.Company \\r\\ninto g\\r\\nselect new\\r\\n{\\r\\n\\tCompany = g.Key,\\r\\n\\tCount = g.Sum(x => x.Count),\\r\\n\\tTotal = g.Sum(x => x.Total)\\r\\n}",
            "Fields": {},
            "Configuration": {
                "Indexing.Static.SearchEngineType": "Corax"
            },
            "SourceType": "Documents",
            "ArchivedDataProcessingBehavior": null,
            "Type": "MapReduce",
            "OutputReduceToCollection": null,
            "ReduceOutputIndex": null,
            "PatternForOutputReduceToCollectionReferences": null,
            "PatternReferencesCollectionName": null,
            "DeploymentMode": null,
            "Name": "Orders/ByCompany",
            "Priority": "Normal",
            "State": null
        },
        "Products/ByUnitOnStock": {
            "ClusterState": {
                "LastIndex": 36,
                "LastStateIndex": 0,
                "LastRollingDeploymentIndex": 0
            },
            "LockMode": "Unlock",
            "AdditionalSources": {},
            "CompoundFields": [],
            "AdditionalAssemblies": [],
            "Maps": [
                "from product in docs.Products\\r\\nselect new {\\r\\n    UnitOnStock = LoadCompareExchangeValue(Id(product))\\r\\n}"
            ],
            "Reduce": null,
            "Fields": {},
            "Configuration": {},
            "SourceType": "Documents",
            "ArchivedDataProcessingBehavior": null,
            "Type": "Map",
            "OutputReduceToCollection": null,
            "ReduceOutputIndex": null,
            "PatternForOutputReduceToCollectionReferences": null,
            "PatternReferencesCollectionName": null,
            "DeploymentMode": null,
            "Name": "Products/ByUnitOnStock",
            "Priority": "Normal",
            "State": null
        },
        "Product/Rating": {
            "ClusterState": {
                "LastIndex": 36,
                "LastStateIndex": 0,
                "LastRollingDeploymentIndex": 0
            },
            "LockMode": "Unlock",
            "AdditionalSources": {},
            "CompoundFields": [],
            "AdditionalAssemblies": [],
            "Maps": [
                "from counter in counters.Products\\r\\nlet product = LoadDocument(counter.DocumentId, \\"Products\\")\\r\\nwhere counter.Name.Contains(\\"⭐\\")\\r\\nselect new {\\r\\n    Name = product.Name,\\r\\n    Rating = counter.Name.Length,\\r\\n    TotalVotes = counter.Value,\\r\\n    AllRatings = new []\\r\\n    {\\r\\n        new\\r\\n        {\\r\\n            Rating = counter.Name,\\r\\n            Votes = counter.Value\\r\\n        }\\r\\n    }\\r\\n}"
            ],
            "Reduce": "from result in results\\r\\ngroup result by result.Name into g\\r\\nlet totalVotes = g.Sum(x => x.TotalVotes)\\r\\nlet rating = g.Sum(x => x.TotalVotes / (double)totalVotes * x.Rating)\\r\\nselect new {\\r\\n   Name = g.Key,\\r\\n   Rating = rating,\\r\\n   TotalVotes = totalVotes,\\r\\n   AllRatings = g.SelectMany(x => x.AllRatings).ToArray()\\r\\n}",
            "Fields": {},
            "Configuration": {},
            "SourceType": "Counters",
            "ArchivedDataProcessingBehavior": "IncludeArchived",
            "Type": "MapReduce",
            "OutputReduceToCollection": null,
            "ReduceOutputIndex": null,
            "PatternForOutputReduceToCollectionReferences": null,
            "PatternReferencesCollectionName": null,
            "DeploymentMode": null,
            "Name": "Product/Rating",
            "Priority": "Normal",
            "State": null
        },
        "Orders/Totals": {
            "ClusterState": {
                "LastIndex": 36,
                "LastStateIndex": 0,
                "LastRollingDeploymentIndex": 0
            },
            "LockMode": "Unlock",
            "AdditionalSources": {},
            "CompoundFields": [],
            "AdditionalAssemblies": [],
            "Maps": [
                "from order in docs.Orders\\r\\nselect new\\r\\n{\\r\\n    order.Employee,\\r\\n    order.Company,\\r\\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\\r\\n}"
            ],
            "Reduce": null,
            "Fields": {},
            "Configuration": {},
            "SourceType": "Documents",
            "ArchivedDataProcessingBehavior": null,
            "Type": "Map",
            "OutputReduceToCollection": null,
            "ReduceOutputIndex": null,
            "PatternForOutputReduceToCollectionReferences": null,
            "PatternReferencesCollectionName": null,
            "DeploymentMode": null,
            "Name": "Orders/Totals",
            "Priority": "Normal",
            "State": null
        },
        "Companies/StockPrices/TradeVolumeByMonth": {
            "ClusterState": {
                "LastIndex": 36,
                "LastStateIndex": 0,
                "LastRollingDeploymentIndex": 0
            },
            "LockMode": "Unlock",
            "AdditionalSources": {},
            "CompoundFields": [],
            "AdditionalAssemblies": [],
            "Maps": [
                "from segment in timeseries.Companies.StockPrices\\r\\nlet company = LoadDocument(segment.DocumentId, \\"Companies\\")\\r\\nfrom entry in segment.Entries\\r\\nselect new \\r\\n{\\r\\n    Date = new DateTime(entry.Timestamp.Year, entry.Timestamp.Month, 1),\\r\\n    Country = company.Address.Country,\\r\\n    Volume = entry.Values[4]\\r\\n}"
            ],
            "Reduce": "from result in results\\r\\ngroup result by new { result.Date, result.Country } into g\\r\\nselect new {\\r\\n    Date = g.Key.Date,\\r\\n    Country = g.Key.Country,\\r\\n    Volume = g.Sum(x => x.Volume)\\r\\n}",
            "Fields": {},
            "Configuration": {},
            "SourceType": "TimeSeries",
            "ArchivedDataProcessingBehavior": "IncludeArchived",
            "Type": "MapReduce",
            "OutputReduceToCollection": null,
            "ReduceOutputIndex": null,
            "PatternForOutputReduceToCollectionReferences": null,
            "PatternReferencesCollectionName": null,
            "DeploymentMode": null,
            "Name": "Companies/StockPrices/TradeVolumeByMonth",
            "Priority": "Normal",
            "State": null
        }
    },
    "IndexesHistory": {
        "Product/Search": [
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 36,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from p in docs.Products\\r\\nselect new\\r\\n{\\r\\n    p.Name,\\r\\n    p.Category,\\r\\n    p.Supplier,\\r\\n    p.PricePerUnit\\r\\n}"
                    ],
                    "Reduce": null,
                    "Fields": {
                        "Name": {
                            "Storage": null,
                            "Indexing": "Search",
                            "TermVector": "Yes",
                            "Spatial": null,
                            "Analyzer": null,
                            "Suggestions": true
                        }
                    },
                    "Configuration": {},
                    "SourceType": "Documents",
                    "ArchivedDataProcessingBehavior": null,
                    "Type": "Map",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Product/Search",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "Smuggler",
                "CreatedAt": "2023-11-28T10:35:43.1081520Z",
                "RollingDeployment": {}
            }
        ],
        "Orders/ByShipment/Location": [
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 36,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from order in docs.Orders\\r\\nwhere order.ShipTo.Location != null\\r\\nselect new\\r\\n{\\r\\n    order.Employee,\\r\\n    order.Company,\\r\\n    ShipmentLocation = CreateSpatialField(order.ShipTo.Location.Latitude, order.ShipTo.Location.Longitude)\\r\\n}"
                    ],
                    "Reduce": null,
                    "Fields": {
                        "ShipmentLocation": {
                            "Storage": null,
                            "Indexing": null,
                            "TermVector": null,
                            "Spatial": {
                                "Type": "Geography",
                                "Strategy": "GeohashPrefixTree",
                                "MaxTreeLevel": 9,
                                "MinX": -180,
                                "MaxX": 180,
                                "MinY": -90,
                                "MaxY": 90,
                                "Units": "Kilometers"
                            },
                            "Analyzer": null,
                            "Suggestions": null
                        }
                    },
                    "Configuration": {},
                    "SourceType": "Documents",
                    "ArchivedDataProcessingBehavior": null,
                    "Type": "Map",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Orders/ByShipment/Location",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "Smuggler",
                "CreatedAt": "2023-11-28T10:35:43.1081520Z",
                "RollingDeployment": {}
            }
        ],
        "Orders/ByCompany": [
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 38,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from order in docs.Orders\\r\\nselect new\\r\\n{\\r\\n    order.Company,\\r\\n    Count = 1,\\r\\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\\r\\n}"
                    ],
                    "Reduce": "from result in results\\r\\ngroup result by result.Company \\r\\ninto g\\r\\nselect new\\r\\n{\\r\\n\\tCompany = g.Key,\\r\\n\\tCount = g.Sum(x => x.Count),\\r\\n\\tTotal = g.Sum(x => x.Total)\\r\\n}",
                    "Fields": {},
                    "Configuration": {
                        "Indexing.Static.SearchEngineType": "Corax"
                    },
                    "SourceType": "Documents",
                    "ArchivedDataProcessingBehavior": null,
                    "Type": "MapReduce",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Orders/ByCompany",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "macbook-pro-mateusz",
                "CreatedAt": "2023-11-28T10:36:22.7556850Z",
                "RollingDeployment": null
            },
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 36,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from order in docs.Orders\\r\\nselect new\\r\\n{\\r\\n    order.Company,\\r\\n    Count = 1,\\r\\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\\r\\n}"
                    ],
                    "Reduce": "from result in results\\r\\ngroup result by result.Company \\r\\ninto g\\r\\nselect new\\r\\n{\\r\\n\\tCompany = g.Key,\\r\\n\\tCount = g.Sum(x => x.Count),\\r\\n\\tTotal = g.Sum(x => x.Total)\\r\\n}",
                    "Fields": {},
                    "Configuration": {},
                    "SourceType": "Documents",
                    "ArchivedDataProcessingBehavior": null,
                    "Type": "MapReduce",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Orders/ByCompany",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "Smuggler",
                "CreatedAt": "2023-11-28T10:35:43.1081520Z",
                "RollingDeployment": {}
            }
        ],
        "Products/ByUnitOnStock": [
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 36,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from product in docs.Products\\r\\nselect new {\\r\\n    UnitOnStock = LoadCompareExchangeValue(Id(product))\\r\\n}"
                    ],
                    "Reduce": null,
                    "Fields": {},
                    "Configuration": {},
                    "SourceType": "Documents",
                    "ArchivedDataProcessingBehavior": null,
                    "Type": "Map",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Products/ByUnitOnStock",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "Smuggler",
                "CreatedAt": "2023-11-28T10:35:43.1081520Z",
                "RollingDeployment": {}
            }
        ],
        "Product/Rating": [
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 36,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from counter in counters.Products\\r\\nlet product = LoadDocument(counter.DocumentId, \\"Products\\")\\r\\nwhere counter.Name.Contains(\\"⭐\\")\\r\\nselect new {\\r\\n    Name = product.Name,\\r\\n    Rating = counter.Name.Length,\\r\\n    TotalVotes = counter.Value,\\r\\n    AllRatings = new []\\r\\n    {\\r\\n        new\\r\\n        {\\r\\n            Rating = counter.Name,\\r\\n            Votes = counter.Value\\r\\n        }\\r\\n    }\\r\\n}"
                    ],
                    "Reduce": "from result in results\\r\\ngroup result by result.Name into g\\r\\nlet totalVotes = g.Sum(x => x.TotalVotes)\\r\\nlet rating = g.Sum(x => x.TotalVotes / (double)totalVotes * x.Rating)\\r\\nselect new {\\r\\n   Name = g.Key,\\r\\n   Rating = rating,\\r\\n   TotalVotes = totalVotes,\\r\\n   AllRatings = g.SelectMany(x => x.AllRatings).ToArray()\\r\\n}",
                    "Fields": {},
                    "Configuration": {},
                    "SourceType": "Counters",
                    "ArchivedDataProcessingBehavior": "IncludeArchived",
                    "Type": "MapReduce",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Product/Rating",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "Smuggler",
                "CreatedAt": "2023-11-28T10:35:43.1081520Z",
                "RollingDeployment": {}
            }
        ],
        "Orders/Totals": [
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 36,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from order in docs.Orders\\r\\nselect new\\r\\n{\\r\\n    order.Employee,\\r\\n    order.Company,\\r\\n    Total = order.Lines.Sum(l => (l.Quantity * l.PricePerUnit) * (1 - l.Discount))\\r\\n}"
                    ],
                    "Reduce": null,
                    "Fields": {},
                    "Configuration": {},
                    "SourceType": "Documents",
                    "ArchivedDataProcessingBehavior": null,
                    "Type": "Map",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Orders/Totals",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "Smuggler",
                "CreatedAt": "2023-11-28T10:35:43.1081520Z",
                "RollingDeployment": {}
            }
        ],
        "Companies/StockPrices/TradeVolumeByMonth": [
            {
                "Definition": {
                    "ClusterState": {
                        "LastIndex": 36,
                        "LastStateIndex": 0,
                        "LastRollingDeploymentIndex": 0
                    },
                    "LockMode": "Unlock",
                    "AdditionalSources": {},
                    "CompoundFields": [],
                    "AdditionalAssemblies": [],
                    "Maps": [
                        "from segment in timeseries.Companies.StockPrices\\r\\nlet company = LoadDocument(segment.DocumentId, \\"Companies\\")\\r\\nfrom entry in segment.Entries\\r\\nselect new \\r\\n{\\r\\n    Date = new DateTime(entry.Timestamp.Year, entry.Timestamp.Month, 1),\\r\\n    Country = company.Address.Country,\\r\\n    Volume = entry.Values[4]\\r\\n}"
                    ],
                    "Reduce": "from result in results\\r\\ngroup result by new { result.Date, result.Country } into g\\r\\nselect new {\\r\\n    Date = g.Key.Date,\\r\\n    Country = g.Key.Country,\\r\\n    Volume = g.Sum(x => x.Volume)\\r\\n}",
                    "Fields": {},
                    "Configuration": {},
                    "SourceType": "TimeSeries",
                    "ArchivedDataProcessingBehavior": "IncludeArchived",
                    "Type": "MapReduce",
                    "OutputReduceToCollection": null,
                    "ReduceOutputIndex": null,
                    "PatternForOutputReduceToCollectionReferences": null,
                    "PatternReferencesCollectionName": null,
                    "DeploymentMode": null,
                    "Name": "Companies/StockPrices/TradeVolumeByMonth",
                    "Priority": "Normal",
                    "State": null
                },
                "Source": "Smuggler",
                "CreatedAt": "2023-11-28T10:35:43.1081520Z",
                "RollingDeployment": {}
            }
        ]
    },
    "AutoIndexes": {},
    "Settings": {},
    "Revisions": {
        "Default": null,
        "Collections": {
            "Orders": {
                "MinimumRevisionsToKeep": null,
                "MinimumRevisionAgeToKeep": null,
                "Disabled": false,
                "PurgeOnDelete": false,
                "MaximumRevisionsToDeleteUponDocumentUpdate": null
            }
        }
    },
    "TimeSeries": {
        "Collections": {},
        "PolicyCheckFrequency": null,
        "NamedValues": {
            "Companies": {
                "StockPrices": [
                    "Open",
                    "Close",
                    "High",
                    "Low",
                    "Volume"
                ]
            },
            "Employees": {
                "HeartRates": [
                    "BPM"
                ]
            }
        }
    },
    "RevisionsForConflicts": null,
    "Expiration": null,
    "Refresh": null,
    "DataArchival": null,
    "Integrations": null,
    "PeriodicBackups": [],
    "ExternalReplications": [],
    "SinkPullReplications": [],
    "HubPullReplications": [],
    "RavenConnectionStrings": {},
    "SqlConnectionStrings": {},
    "OlapConnectionStrings": {},
    "ElasticSearchConnectionStrings": {},
    "QueueConnectionStrings": {},
    "RavenEtls": [],
    "SqlEtls": [],
    "ElasticSearchEtls": [],
    "OlapEtls": [],
    "QueueEtls": [],
    "QueueSinks": [],
    "Client": null,
    "Studio": null,
    "TruncatedClusterTransactionCommandsCount": 0,
    "UnusedDatabaseIds": [],
    "Etag": 38
}`;
