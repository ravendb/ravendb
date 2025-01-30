import { Button, Col, Row } from "reactstrap";
import React from "react";
import { AboutViewHeading } from "components/common/AboutView";
import VirtualTable from "components/common/virtualTable/VirtualTable";
import { getCoreRowModel, getFilteredRowModel, getSortedRowModel, useReactTable } from "@tanstack/react-table";
import { useDocumentIdentitiesColumns } from "components/pages/database/documents/identities/useDocumentIdentitiesColumns";
import SizeGetter from "components/common/SizeGetter";
import DocumentIdentitiesModal from "components/pages/database/documents/identities/DocumentIdentitiesModal";
import DocumentIdentitiesAboutView from "components/pages/database/documents/identities/DocumentIdentitiesAboutView";
import { useServices } from "hooks/useServices";
import { AsyncStateStatus, useAsync } from "react-async-hook";
import { Icon } from "components/common/Icon";
import { useAppSelector } from "components/store";
import { accessManagerSelectors } from "components/common/shell/accessManagerSliceSelectors";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import useBoolean from "hooks/useBoolean";
import { LoadError } from "components/common/LoadError";
import { AddIdentitiesFormData } from "components/pages/database/documents/identities/DocumentIdentitiesValidation";

export default function DocumentIdentities() {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);
    const { identities, reload, status, isLoading } = useGetIdentities();

    return (
        <div className="content-padding h-100">
            <Row className="gy-sm h-100">
                <Col>
                    <DocumentIdentitiesWithSize
                        setIsOpen={toggleIsOpen}
                        identities={identities}
                        reload={reload}
                        status={status}
                        isLoading={isLoading}
                    />
                </Col>
                <Col sm={12} md={4}>
                    <DocumentIdentitiesAboutView />
                </Col>
                <DocumentIdentitiesModal
                    identities={identities}
                    refetch={reload}
                    isOpen={isOpen}
                    toggleModal={toggleIsOpen}
                />
            </Row>
        </div>
    );
}

interface DocumentIdentitiesWithSizeProps {
    setIsOpen: () => void;
    identities: AddIdentitiesFormData[];
    isLoading: boolean;
    status: AsyncStateStatus;
    reload: () => void;
}

function DocumentIdentitiesWithSize({
    setIsOpen,
    identities,
    isLoading,
    reload,
    status,
}: DocumentIdentitiesWithSizeProps) {
    const hasDatabaseAccessWrite = useAppSelector(accessManagerSelectors.getHasDatabaseWriteAccess)();

    return (
        <div className="h-100 vstack">
            <AboutViewHeading title="Identities" icon="identities" />
            <div className="d-flex mb-3">
                <Button color="primary" onClick={setIsOpen} disabled={!hasDatabaseAccessWrite} title="Add new identity">
                    <Icon icon="plus" />
                    Add new identity
                </Button>
            </div>
            <SizeGetter
                render={(props) => (
                    <DocumentIdentitiesTable
                        identities={identities}
                        reload={reload}
                        status={status}
                        isLoading={isLoading}
                        {...props}
                    />
                )}
            />
        </div>
    );
}

interface DocumentIdentitiesTableProps {
    status: AsyncStateStatus;
    reload: () => void;
    identities: AddIdentitiesFormData[];
    width: number;
    height: number;
    isLoading: boolean;
}

function DocumentIdentitiesTable({
    status,
    identities,
    width,
    reload,
    height,
    isLoading,
}: DocumentIdentitiesTableProps) {
    const { identitiesColumns } = useDocumentIdentitiesColumns(width, reload);

    const identitiesTable = useReactTable({
        columns: identitiesColumns,
        data: identities,
        columnResizeMode: "onChange",
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
    });

    return (
        <>
            {status === "error" ? (
                <LoadError error="Error during loading identites" refresh={() => reload()} />
            ) : (
                <VirtualTable heightInPx={height} table={identitiesTable} isLoading={isLoading} />
            )}
        </>
    );
}

function useGetIdentities() {
    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const { databasesService } = useServices();
    const { loading, result, execute, status } = useAsync(async () => {
        const result = await databasesService.getIdentities(databaseName);

        return Object.keys(result).map((identity) => ({
            prefix: identity,
            value: result[identity],
        }));
    }, []);

    return {
        identities: result ?? [],
        isLoading: loading,
        reload: execute,
        status: status,
    };
}
