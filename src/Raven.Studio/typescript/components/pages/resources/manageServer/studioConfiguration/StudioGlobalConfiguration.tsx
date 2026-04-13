import { createContext, useContext, useMemo, useState } from "react";
import Card from "react-bootstrap/Card";
import Form from "react-bootstrap/Form";
import Row from "react-bootstrap/Row";
import Col from "react-bootstrap/Col";

import { SubmitHandler, useForm } from "react-hook-form";
import { FormInput, FormLabel, FormSelect, FormSelectCreatable, FormSwitch } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { Icon } from "components/common/Icon";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import {
    StudioGlobalConfigurationFormData,
    studioGlobalConfigurationYupResolver,
} from "./StudioGlobalConfigurationValidation";
import { useDirtyFlag } from "components/hooks/useDirtyFlag";
import { useEventsCollector } from "components/hooks/useEventsCollector";
import { useAsyncCallback } from "react-async-hook";
import { LoadingView } from "components/common/LoadingView";
import { LoadError } from "components/common/LoadError";
import {
    predefinedFontOptions,
    studioEnvironmentOptions,
} from "components/common/studioConfiguration/StudioConfigurationUtils";
import { SelectOption } from "components/common/select/Select";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import { useAppSelector } from "components/store";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useRavenLink } from "components/hooks/useRavenLink";
import FeatureAvailabilitySummaryWrapper, {
    FeatureAvailabilityData,
} from "components/common/FeatureAvailabilitySummary";
import { useLimitedFeatureAvailability } from "components/utils/licenseLimitsUtils";
import FeatureNotAvailableInYourLicensePopoverBody from "components/common/FeatureNotAvailableInYourLicensePopoverBody";
import PopoverWithHoverWrapper from "components/common/PopoverWithHoverWrapper";
import { ConditionalPopover } from "components/common/ConditionalPopover";
import { components as rsComponents, MenuProps, OptionProps } from "react-select";
import { ColumnDef, getCoreRowModel, useReactTable } from "@tanstack/react-table";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { CellValueWrapper } from "components/common/virtualTable/cells/CellValue";
import { virtualTableUtils } from "components/common/virtualTable/utils/virtualTableUtils";
import AceEditor from "components/common/ace/AceEditor";
import studioSettings = require("common/settings/studioSettings");

export default function StudioGlobalConfiguration() {
    const [vtHoveredFont, setVtHoveredFont] = useState<string | null>(null);
    const [codeHoveredFont, setCodeHoveredFont] = useState<string | null>(null);

    const asyncGlobalSettings = useAsyncCallback<StudioGlobalConfigurationFormData>(async () => {
        const settings = await studioSettings.default.globalSettings(true);

        return {
            environment: settings.environment.getValue(),
            replicationFactor: settings.replicationFactor.getValue(),
            isCollapseDocsWhenOpening: settings.collapseDocsWhenOpening.getValue(),
            isSendUsageStats: settings.sendUsageStats.getValue(),
            tableFont: settings.tableFont.getValue(),
            monospaceFont: settings.monospaceFont.getValue(),
        };
    });

    const { handleSubmit, control, formState, reset } = useForm<StudioGlobalConfigurationFormData>({
        resolver: studioGlobalConfigurationYupResolver,
        mode: "all",
        defaultValues: asyncGlobalSettings.execute,
    });

    useDirtyFlag(formState.isDirty);

    const clientConfigurationDocsLink = useRavenLink({ hash: "HIR1VP" });
    const { reportEvent } = useEventsCollector();

    const hasStudioConfiguration = useAppSelector(licenseSelectors.statusValue("HasStudioConfiguration"));
    const featureAvailability = useLimitedFeatureAvailability({
        defaultFeatureAvailability,
        overwrites: [
            {
                featureName: defaultFeatureAvailability[0].featureName,
                value: hasStudioConfiguration,
            },
        ],
    });

    const onSave: SubmitHandler<StudioGlobalConfigurationFormData> = async (formData) => {
        return tryHandleSubmit(async () => {
            reportEvent("studio-configuration-global", "save");
            const settings = await studioSettings.default.globalSettings();

            settings.environment.setValueLazy(formData.environment);
            settings.replicationFactor.setValueLazy(formData.replicationFactor);
            settings.collapseDocsWhenOpening.setValue(formData.isCollapseDocsWhenOpening);
            settings.sendUsageStats.setValueLazy(formData.isSendUsageStats);
            settings.tableFont.setValue(formData.tableFont);
            settings.monospaceFont.setValue(formData.monospaceFont);

            await settings.save();
            reset(formData);
        });
    };

    const onRefresh = async () => {
        reset(await asyncGlobalSettings.execute());
    };

    if (asyncGlobalSettings.status === "not-requested" || asyncGlobalSettings.status === "loading") {
        return <LoadingView />;
    }

    if (asyncGlobalSettings.status === "error") {
        return <LoadError error="Unable to load studio configuration" refresh={onRefresh} />;
    }

    return (
        <div className="content-margin">
            <Row className="gy-sm">
                <Col>
                    <AboutViewHeading
                        icon="studio-configuration"
                        title="Studio Configuration"
                        licenseBadgeText={hasStudioConfiguration ? null : "Professional +"}
                    />
                    <Form onSubmit={handleSubmit(onSave)} autoComplete="off">
                        <ConditionalPopover
                            conditions={{
                                isActive: !hasStudioConfiguration,
                                message: <FeatureNotAvailableInYourLicensePopoverBody />,
                            }}
                        >
                            <ButtonWithSpinner
                                type="submit"
                                variant="primary"
                                className="mb-3"
                                icon="save"
                                disabled={!formState.isDirty}
                                isSpinning={formState.isSubmitting}
                            >
                                Save
                            </ButtonWithSpinner>
                        </ConditionalPopover>
                        <div className={hasStudioConfiguration ? null : "item-disabled pe-none"}>
                            <Card>
                                <Card.Body className="vstack gap-3">
                                    <div className="gap-1">
                                        <FormLabel className="mb-0 md-label">
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <ul>
                                                        <li className="margin-bottom-xs">
                                                            Apply a <strong>tag</strong> to the Studio indicating the
                                                            server environment.
                                                        </li>
                                                        <li>This does not affect any settings or features.</li>
                                                    </ul>
                                                }
                                                placement="right"
                                            >
                                                Server Environment <Icon icon="info-new" id="EnvironmentInfo" />
                                            </PopoverWithHoverWrapper>
                                        </FormLabel>
                                        <FormSelect
                                            control={control}
                                            name="environment"
                                            options={studioEnvironmentOptions}
                                            isSearchable={false}
                                        />
                                    </div>
                                    <div className="gap-1">
                                        <FormLabel className="mb-0 md-label">
                                            Default Replication Factor{" "}
                                            <PopoverWithHoverWrapper
                                                message={
                                                    <ul>
                                                        <li className="margin-bottom-xs">
                                                            Set the default <strong>replication factor</strong> when
                                                            creating a new database.
                                                        </li>
                                                        <li className="margin-bottom-xs">
                                                            {" "}
                                                            If not set, then the number of nodes in your cluster will be
                                                            used.
                                                        </li>
                                                        <li>
                                                            Additional nodes can always be added to the database after
                                                            it is created.
                                                        </li>
                                                    </ul>
                                                }
                                                placement="right"
                                            >
                                                <Icon icon="info-new" id="ReplicationFactorInfo" />
                                            </PopoverWithHoverWrapper>
                                        </FormLabel>
                                        <FormInput
                                            control={control}
                                            name="replicationFactor"
                                            type="number"
                                            placeholder="Cluster size (default)"
                                        />
                                    </div>
                                </Card.Body>
                            </Card>
                            <Card className="mt-3">
                                <Card.Body className="vstack gap-3">
                                    <div className="gap-1">
                                        <FormLabel className="mb-0 md-label">
                                            Table Font{" "}
                                            <PopoverWithHoverWrapper
                                                message={<TableFontPopoverContent />}
                                                placement="right"
                                            >
                                                <Icon icon="info-new" />
                                            </PopoverWithHoverWrapper>
                                        </FormLabel>
                                        <FontHoverContext.Provider
                                            value={{
                                                hoveredFont: vtHoveredFont,
                                                setHoveredFont: setVtHoveredFont,
                                            }}
                                        >
                                            <FormSelectCreatable
                                                control={control}
                                                name="tableFont"
                                                options={predefinedFontOptions}
                                                formatOptionLabel={formatFontOptionLabel}
                                                components={tableFontSelectComponents}
                                                onMenuClose={() => setVtHoveredFont(null)}
                                            />
                                        </FontHoverContext.Provider>
                                    </div>
                                    <div className="gap-1">
                                        <FormLabel className="mb-0 md-label">
                                            Code Font{" "}
                                            <PopoverWithHoverWrapper
                                                message={<CodeFontPopoverContent />}
                                                placement="right"
                                            >
                                                <Icon icon="info-new" />
                                            </PopoverWithHoverWrapper>
                                        </FormLabel>
                                        <FontHoverContext.Provider
                                            value={{
                                                hoveredFont: codeHoveredFont,
                                                setHoveredFont: setCodeHoveredFont,
                                            }}
                                        >
                                            <FormSelectCreatable
                                                control={control}
                                                name="monospaceFont"
                                                options={predefinedFontOptions}
                                                formatOptionLabel={formatFontOptionLabel}
                                                components={codeFontSelectComponents}
                                                onMenuClose={() => setCodeHoveredFont(null)}
                                            />
                                        </FontHoverContext.Provider>
                                    </div>
                                    <div className="d-flex flex-column">
                                        <FormSwitch control={control} name="isCollapseDocsWhenOpening">
                                            Collapse documents when opening
                                        </FormSwitch>
                                        <FormSwitch control={control} name="isSendUsageStats" className="mt-2">
                                            Help improve the Studio by gathering anonymous usage statistics
                                        </FormSwitch>
                                    </div>
                                </Card.Body>
                            </Card>
                        </div>
                    </Form>
                </Col>
                <Col sm={12} md={4}>
                    <AboutViewAnchored defaultOpen={hasStudioConfiguration ? null : "licensing"}>
                        <AccordionItemWrapper
                            icon="about"
                            color="info"
                            heading="About this view"
                            description="Get additional info on this feature"
                            targetId="1"
                        >
                            <ul>
                                <li className="margin-bottom-xs">
                                    This is the <strong>Server-wide Studio-Configuration</strong> view.
                                    <br />
                                    The available studio-configuration options will apply server-wide to all databases.
                                </li>
                                <li>
                                    The environment tag can be customized per database in the Database
                                    Studio-Configuration view.
                                </li>
                            </ul>
                            <hr />
                            <div className="small-label mb-2">useful links</div>
                            <a href={clientConfigurationDocsLink} target="_blank">
                                <Icon icon="newtab" /> Docs - Client Configuration
                            </a>
                        </AccordionItemWrapper>
                        <FeatureAvailabilitySummaryWrapper
                            isUnlimited={hasStudioConfiguration}
                            data={featureAvailability}
                        />
                    </AboutViewAnchored>
                </Col>
            </Row>
        </div>
    );
}

const FontHoverContext = createContext<{
    hoveredFont: string | null;
    setHoveredFont: (font: string | null) => void;
}>({ hoveredFont: null, setHoveredFont: () => {} });

function FontPreviewOption(props: OptionProps<SelectOption<string>>) {
    const { setHoveredFont } = useContext(FontHoverContext);
    return (
        <rsComponents.Option
            {...props}
            innerProps={{
                ...props.innerProps,
                onMouseEnter: (e: React.MouseEvent<HTMLDivElement>) => {
                    props.innerProps.onMouseEnter?.(e);
                    setHoveredFont(props.data.value);
                },
            }}
        />
    );
}

function getPreviewFontFamily(
    hoveredFont: string | null,
    selectProps: MenuProps<SelectOption<string>>["selectProps"],
    defaultFont: string
): string {
    const fontValue = hoveredFont ?? (selectProps.value as SelectOption<string>)?.value ?? "default";
    return fontValue === "default" ? defaultFont : `"${fontValue}"`;
}

function TableFontMenu(props: MenuProps<SelectOption<string>>) {
    const { hoveredFont } = useContext(FontHoverContext);
    const fontFamily = getPreviewFontFamily(hoveredFont, props.selectProps, '"Figtree"');

    return (
        <rsComponents.Menu {...props}>
            <div className="d-flex">
                <div style={{ minWidth: 240, flexShrink: 0 }}>{props.children}</div>
                <div className="vr" />
                <VtTablePreview fontFamily={fontFamily} />
            </div>
        </rsComponents.Menu>
    );
}

function CodeFontMenu(props: MenuProps<SelectOption<string>>) {
    const { hoveredFont } = useContext(FontHoverContext);
    const fontFamily = getPreviewFontFamily(hoveredFont, props.selectProps, "var(--bs-font-monospace)");

    return (
        <rsComponents.Menu {...props}>
            <div className="d-flex">
                <div style={{ minWidth: 240, flexShrink: 0 }}>{props.children}</div>
                <div className="vr" />
                <CodePreview fontFamily={fontFamily} />
            </div>
        </rsComponents.Menu>
    );
}

interface VtPreviewRow {
    name: string;
    active: boolean;
    city: string;
}

const vtPreviewData: VtPreviewRow[] = [
    { name: "3d7b4e1c-9f2a-4b8c-e5d1-2a6c8f9e1b3d", active: true, city: "New York" },
    { name: "7f2c5a9e-1b4d-4a7f-c3e8-5d9b2f1e6c4a", active: false, city: "London" },
    { name: "a8e4b6d2-7c3f-4e9b-1a5d-8f2c6e9b3a7f", active: true, city: "Tokyo" },
    { name: "5c1e9a7b-3d6f-4b2e-9c1a-7e4b8f2d5c9a", active: true, city: "Berlin" },
    { name: "2a7f4d9c-5e1b-4a6f-c8d3-1f9e5a7b2c4e", active: false, city: "Paris" },
    { name: "9b3e6a1f-2d7c-4b9a-e5f1-6c2a8d3f7e9b", active: false, city: "Sydney" },
];

const vtPreviewColumns: ColumnDef<VtPreviewRow>[] = [
    {
        header: "Name",
        accessorKey: "name",
        cell: CellValueWrapper,
        size: 160,
        enableColumnFilter: false,
        enableSorting: false,
    },
    {
        header: "Active",
        accessorKey: "active",
        cell: CellValueWrapper,
        size: 80,
        enableColumnFilter: false,
        enableSorting: false,
    },
    {
        header: "City",
        accessorKey: "city",
        cell: CellValueWrapper,
        size: 120,
        enableColumnFilter: false,
        enableSorting: false,
    },
];

function VtTablePreview({ fontFamily }: { fontFamily: string }) {
    const table = useReactTable({
        data: vtPreviewData,
        columns: vtPreviewColumns,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        enableColumnFilters: false,
    });

    return (
        <div
            className="flex-grow-1 p-2 overflow-hidden font-preview-table"
            style={{ "--preview-font-family": fontFamily, borderRadius: 8 } as React.CSSProperties}
        >
            <span className="md-label">Preview</span>
            <VirtualTable table={table} heightInPx={virtualTableUtils.getHeightInPx(vtPreviewData.length, 300)} />
        </div>
    );
}

const codePreviewValue = `from Orders
where Lines.Count > 3
select {
    Id: id(),
    Company: Company,
    Total: Lines.Sum(x => x.Price)
}`;

function CodePreview({ fontFamily }: { fontFamily: string }) {
    return (
        <div
            className="flex-grow-1 p-2 overflow-hidden"
            style={{ "--monospace-font": fontFamily, borderRadius: 8 } as React.CSSProperties}
        >
            <span className="md-label">Preview</span>
            <AceEditor
                mode="rql"
                value={codePreviewValue}
                readOnly
                height="230px"
                width="400px"
                isFullScreenLabelHidden
                setOptions={{
                    showLineNumbers: true,
                    showPrintMargin: false,
                    highlightGutterLine: false,
                }}
                aceRef={undefined}
            />
        </div>
    );
}

const tableFontSelectComponents = { Menu: TableFontMenu, Option: FontPreviewOption };
const codeFontSelectComponents = { Menu: CodeFontMenu, Option: FontPreviewOption };

function formatFontOptionLabel(option: SelectOption<string>) {
    if (option.value === "default") {
        return <span>{option.label}</span>;
    }
    return <span style={{ fontFamily: `"${option.value}"` }}>{option.label}</span>;
}

function TableFontPopoverContent() {
    return (
        <p className="mb-0">
            Choose the font used for displaying data in <strong>tables</strong> across the Studio, including document
            IDs, column values, and other tabular content.
        </p>
    );
}

function CodeFontPopoverContent() {
    return (
        <p className="mb-0">
            Choose the font used for displaying <strong>code</strong> across the Studio, including code editors and
            samples.
        </p>
    );
}

const defaultFeatureAvailability: FeatureAvailabilityData[] = [
    {
        featureName: "Studio Configuration",
        featureIcon: "studio-configuration",
        community: { value: false },
        professional: { value: true },
        enterprise: { value: true },
    },
];
