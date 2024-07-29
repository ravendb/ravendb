import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { useAppSelector } from "components/store";
import React from "react";
import { useForm, useWatch } from "react-hook-form";
import { RevertRevisionsFormData, revertRevisionsYupResolver } from "./RevertRevisionsValidation";
import { Row, Col, Form, Card, CardBody, Label, FormGroup, InputGroup } from "reactstrap";
import { AboutViewAnchored, AboutViewHeading, AccordionItemWrapper } from "components/common/AboutView";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAppUrls } from "components/hooks/useAppUrls";
import { Icon } from "components/common/Icon";
import { FormDatePicker, FormInput, FormSelect } from "components/common/Form";
import { SelectOption } from "components/common/select/Select";
import assertUnreachable from "components/utils/assertUnreachable";
import moment from "moment";
import useConfirm from "components/common/ConfirmDialog";
import { tryHandleSubmit } from "components/utils/common";
import { useServices } from "components/hooks/useServices";
import notificationCenter from "common/notifications/notificationCenter";
import FormCollectionsSelect from "components/common/FormCollectionsSelect";
import { useAsyncCallback } from "react-async-hook";
import RevertRevisionsRequest = Raven.Server.Documents.Revisions.RevertRevisionsRequest;
import { collectionsTrackerSelectors } from "components/common/shell/collectionsTrackerSlice";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import activeDatabaseTracker = require("common/shell/activeDatabaseTracker");

export default function RevertRevisions() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const hasDatabaseAdminAccess = useAppSelector(accessManagerSelectors.getHasDatabaseAdminAccess)();

    const { control, formState, handleSubmit, setValue } = useForm<RevertRevisionsFormData>({
        resolver: revertRevisionsYupResolver,
        defaultValues: {
            pointInTime: null,
            timeWindow: null,
            timeMagnitude: "hours",
            isRevertAllCollections: true,
            collections: [],
        },
    });

    const { isRevertAllCollections, collections, pointInTime } = useWatch({ control });

    const { forCurrentDatabase } = useAppUrls();
    const confirm = useConfirm();

    const formattedPointInTimeUtc = moment(pointInTime).utc().format(defaultDateFormat) + " UTC";

    const { databasesService } = useServices();

    const asyncRevertRevisions = useAsyncCallback((dto: RevertRevisionsRequest) =>
        databasesService.revertRevisions(databaseName, dto)
    );

    const allCollectionNames = useAppSelector(collectionsTrackerSelectors.collectionNames).filter(
        (x) => x !== "@empty" && x !== "@hilo"
    );

    const onRevert = async (formData: RevertRevisionsFormData) => {
        const isConfirmed = await confirm({
            title: `Do you want to revert documents state to date: ${formattedPointInTimeUtc}?`,
            icon: "revert-revisions",
            actionColor: "primary",
            confirmText: "Revert",
        });

        if (isConfirmed) {
            return tryHandleSubmit(async () => {
                const result = await asyncRevertRevisions.execute(toDto(formData));
                notificationCenter.instance.openDetailsForOperationById(
                    activeDatabaseTracker.default.database(),
                    result.OperationId
                );
            });
        }
    };

    return (
        <Row className="content-margin gy-sm">
            <Col>
                <AboutViewHeading title="Revert Revisions" icon="revert-revisions" />
                <Form onSubmit={handleSubmit(onRevert)} autoComplete="off">
                    <div className="d-flex justify-content-between align-items-end">
                        <ButtonWithSpinner
                            type="submit"
                            color="primary"
                            icon="revert-revisions"
                            disabled={!formState.isDirty}
                            isSpinning={asyncRevertRevisions.status === "loading"}
                        >
                            Revert
                        </ButtonWithSpinner>
                        <small>
                            <a href={forCurrentDatabase.revisions()} title="Navigate to Document Revisions View">
                                <Icon icon="link" />
                                Go back to Revisions View
                            </a>
                        </small>
                    </div>
                    <Card className="mt-3">
                        <CardBody className="gap-4">
                            <FormGroup>
                                <Label for="pointInTime">Point in Time</Label>
                                <FormDatePicker
                                    id="pointInTime"
                                    name="pointInTime"
                                    control={control}
                                    showTimeSelect
                                    timeIntervals={15}
                                    filterDate={filterPointInTime}
                                    filterTime={filterPointInTime}
                                    showYearDropdown
                                    showMonthDropdown
                                    placeholderText="Select the point in time"
                                    addon="local"
                                />
                            </FormGroup>
                            <FormGroup>
                                <Label for="timeWindow">Time Window</Label>
                                <InputGroup>
                                    <FormInput
                                        type="number"
                                        control={control}
                                        id="timeWindow"
                                        name="timeWindow"
                                        placeholder={`default (${defaultWindowValue})`}
                                        addon={
                                            <FormSelect
                                                control={control}
                                                name="timeMagnitude"
                                                options={timeWindowOptions}
                                                isSearchable={false}
                                                className="w-25"
                                            />
                                        }
                                    />
                                </InputGroup>
                            </FormGroup>
                        </CardBody>
                    </Card>
                    {hasDatabaseAdminAccess && (
                        <Card className="mt-3">
                            <CardBody>
                                <FormCollectionsSelect
                                    control={control}
                                    collectionsFormName="collections"
                                    collections={collections}
                                    isAllCollectionsFormName="isRevertAllCollections"
                                    isAllCollections={isRevertAllCollections}
                                    allCollectionNames={allCollectionNames}
                                    setValue={setValue}
                                />
                            </CardBody>
                        </Card>
                    )}
                </Form>
            </Col>
            <Col sm={12} lg={4}>
                <AboutViewAnchored defaultOpen="about-view">
                    <AccordionItemWrapper
                        targetId="about-view"
                        icon="about"
                        color="info"
                        description="Get additional info on this feature"
                        heading="About this view"
                    >
                        {pointInTime && (
                            <div className="flex-horizontal margin-bottom">
                                <div>
                                    When &apos;Revert Revisions&apos; is executed the following rules are applied:
                                    <ul>
                                        <li>
                                            Documents
                                            <strong>
                                                <em> modified </em>
                                            </strong>
                                            after Point in Time:
                                            <code> {formattedPointInTimeUtc} </code>
                                            will be reverted (by creating new revision) to latest version before
                                            <code> {formattedPointInTimeUtc} </code>.
                                        </li>
                                        <li>
                                            If collection has maximum revisions limit and all of them were
                                            <strong>
                                                <em> created </em>
                                            </strong>
                                            after Point in Time:
                                            <code> {formattedPointInTimeUtc} </code>
                                            the oldest revision will be used.
                                        </li>
                                        <li>
                                            Documents
                                            <strong>
                                                <em> created </em>
                                            </strong>
                                            after Point in Time:
                                            <code> {formattedPointInTimeUtc} </code>
                                            will be moved to
                                            <strong>
                                                <em> Revisions&nbsp;Bin</em>
                                            </strong>
                                            .
                                        </li>
                                        <li>Remaining documents will not be modified.</li>
                                    </ul>
                                </div>
                            </div>
                        )}
                        <div>
                            <strong>Time Window</strong> parameter is used for performance optimization: since revisions
                            are not sorted by date, we stop the revert process when hitting a versioned document outside
                            the window.
                        </div>
                    </AccordionItemWrapper>
                </AboutViewAnchored>
            </Col>
        </Row>
    );
}

const defaultDateFormat = "DD/MM/YYYY HH:mm";
const defaultWindowValue = 96;

const filterPointInTime = (date: Date) => {
    const currentDate = moment().add(10, "minutes");
    const selectedDate = moment(date);

    return currentDate > selectedDate;
};

const timeWindowOptions: SelectOption<timeMagnitude>[] = ["minutes", "hours", "days"].map((x: timeMagnitude) => ({
    value: x,
    label: x,
}));

function toDto(formData: RevertRevisionsFormData): RevertRevisionsRequest {
    let WindowInSec = formData.timeWindow ?? defaultWindowValue;

    switch (formData.timeMagnitude) {
        case "minutes":
            WindowInSec *= 60;
            break;
        case "hours":
            WindowInSec *= 3600;
            break;
        case "days":
            WindowInSec *= 24 * 3600;
            break;
        default:
            assertUnreachable(formData.timeMagnitude);
    }

    return {
        Time: moment(formData.pointInTime).utc().toISOString(),
        WindowInSec,
        Collections: formData.isRevertAllCollections ? [] : formData.collections,
    };
}
