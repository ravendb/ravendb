import { Button, Card, CardBody, Col, Input, Row } from "reactstrap";
import React, { ChangeEvent, useState } from "react";
import { AboutViewHeading } from "components/common/AboutView";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import { useDocumentIdentitiesColumns } from "components/pages/database/documents/identities/useDocumentIdentitiesColumns";
import SizeGetter from "components/common/SizeGetter";
import DocumentIdentitiesAddModal from "components/pages/database/documents/identities/DocumentIdentitiesAddModal";
import DocumentIdentitiesAboutView from "components/pages/database/documents/identities/DocumentIdentitiesAboutView";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import { debounce } from "lodash";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";

interface DocumentIdentitiesWithSizeProps {
    width: number;
}

export default function DocumentIdentities() {
    return (
        <div className="content-padding">
            <SizeGetter render={({ width }) => <DocumentIdentitiesWithSize width={width} />} />
        </div>
    );
}

interface Identity {
    prefix: string;
    value: number;
}

function DocumentIdentitiesWithSize({ width }: DocumentIdentitiesWithSizeProps) {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);

    const { identitiesColumns } = useDocumentIdentitiesColumns(width);
    const databaseAccessWrite = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();
    const [isOpen, setIsOpen] = React.useState(false);
    const [globalFilter, setGlobalFilter] = React.useState("");

    const toggleModal = (value: boolean) => setIsOpen(value);

    const { loading, identities } = useGetIdentities(databaseName);

    const identitiesTable = useReactTable({
        columns: identitiesColumns,
        data: identities,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        onGlobalFilterChange: setGlobalFilter,
        globalFilterFn: "includesString",
        filterFns: {},
        state: {
            globalFilter,
        },
    });

    const debouncedSearch = debounce((e: ChangeEvent<HTMLInputElement>) => setGlobalFilter(e.target.value), 300);

    return (
        <>
            <div className="content-margin">
                <Col xs={12} md={12} lg={7}>
                    <AboutViewHeading title="Identities" icon="identities" />
                    <Row className="justify-content-between rows-cols-lg-2 row-cols-1 mb-4">
                        <Col md={12} lg={8} xl={9}>
                            <Button
                                color="primary"
                                onClick={() => setIsOpen(true)}
                                disabled={!databaseAccessWrite}
                                className="add-new-identity-btn py-2"
                            >
                                <Icon icon="plus" className="mr-2" />
                                Add new identity
                            </Button>
                        </Col>
                        <Col md={12} lg={4} xl={3}>
                            <Input onChange={debouncedSearch} placeholder="Filter prefix" />
                        </Col>
                    </Row>

                    <Card>
                        <CardBody>
                            <VirtualTable
                                table={identitiesTable}
                                className="mt-3"
                                isLoading={loading}
                                heightInPx={800}
                            />
                        </CardBody>
                    </Card>
                </Col>
                <Col xs={12} md={12} lg={5}>
                    <DocumentIdentitiesAboutView />
                </Col>
            </div>
            <DocumentIdentitiesAddModal isOpen={isOpen} toggleModal={toggleModal} />
        </>
    );
}

function useGetIdentities(database: string) {
    const [identities, setIdentities] = useState<Identity[]>([]);
    const { databasesService } = useServices();
    const asyncGetIdentities = useAsync(() => databasesService.getIdentities(database), [], {
        onSuccess: (result) =>
            setIdentities(
                Object.keys(result)
                    .map((identity) => ({
                        prefix: identity,
                        value: result[identity],
                    }))
                    .filter(Boolean)
            ),
    });

    return { identities, status: asyncGetIdentities.status, loading: asyncGetIdentities.loading };
}
