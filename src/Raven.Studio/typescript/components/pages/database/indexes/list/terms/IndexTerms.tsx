import { Icon } from "components/common/Icon";
import {
    AccordionBody,
    AccordionHeader,
    AccordionItem,
    Badge,
    Button,
    Col,
    Row,
    UncontrolledAccordion,
} from "reactstrap";
import { UseAsyncReturn } from "react-async-hook";
import React from "react";
import "./IndexTerms.scss";
import copyToClipboard from "common/copyToClipboard";
import queryCriteria from "models/database/query/queryCriteria";
import queryUtil from "common/queryUtil";
import savedQueriesStorage from "common/storage/savedQueriesStorage";
import { LoadingView } from "components/common/LoadingView";
import useBoolean from "hooks/useBoolean";
import { TermsForField, useIndexTerms } from "components/pages/database/indexes/list/terms/useIndexTerms";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import IndexTermsPreviewModal from "components/pages/database/indexes/list/terms/IndexTermsPreviewModal";
import { useAppSelector } from "components/store";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { EmptySet } from "components/common/EmptySet";
import { LoadError } from "components/common/LoadError";

export default function IndexTerms({ pathParams }: ReactProps) {
    const indexName = pathParams[0];

    const { asyncGetIndexEntriesFields, indexTerms, termsLoadedAmount, loadMore, editUrl } = useIndexTerms(indexName);

    if (asyncGetIndexEntriesFields.error) {
        return <LoadError error="Error during getting fields" refresh={asyncGetIndexEntriesFields.execute} />;
    }

    return (
        <div className="index-terms content-margin">
            <Row>
                <Col className="d-flex align-items-center gap-2">
                    <Icon icon="terms" margin="m-0" />
                    <span>
                        Index terms for <a href={editUrl}>{indexName}</a>
                    </span>
                    <Badge color="primary">{termsLoadedAmount} loaded</Badge>
                </Col>
            </Row>
            <Row className="mt-4">
                {asyncGetIndexEntriesFields.loading && <LoadingView />}
                {!asyncGetIndexEntriesFields.loading && indexTerms.length === 0 && (
                    <EmptySet iconSize="lg">No fields were found.</EmptySet>
                )}
                {indexTerms.map((field) => (
                    <IndexTermsAccordions key={field.name} field={field} indexName={indexName} loadMore={loadMore} />
                ))}
            </Row>
        </div>
    );
}

interface IndexTermsAccordionsProps {
    field: TermsForField;
    indexName: string;
    loadMore: UseAsyncReturn<TermsForField, [fieldName: string]>;
}

function IndexTermsAccordions({ field, indexName, loadMore }: IndexTermsAccordionsProps) {
    return (
        <UncontrolledAccordion
            key={field.name}
            data-testid="term-accordion"
            className="bs5 mt-3 accordion-inside-modal"
            id={field.name}
            stayOpen
            flush
            toggle={null}
        >
            <AccordionItem>
                <AccordionHeader targetId={field.name}>
                    <div className="d-flex align-items-center gap-2">
                        <span className="m-0">{field.name}</span>
                        {field.type === "dynamic" && (
                            <Badge data-testid="term-dynamic-field" color="muted">
                                Dynamic field
                            </Badge>
                        )}
                        <Badge pill>{field.terms.length}{field.hasMoreTerms ? '+' : ''}</Badge>
                    </div>
                </AccordionHeader>
                <AccordionBody accordionId={field.name}>
                    {field.terms.length === 0 && <EmptySet iconSize="lg">No entries were found.</EmptySet>}
                    <div>
                        {field.terms.map((term, index) => (
                            <IndexTermItem
                                key={index}
                                term={term}
                                index={index}
                                fieldTerms={field.terms}
                                indexName={indexName}
                                field={field}
                            />
                        ))}
                    </div>
                    {field.hasMoreTerms && (
                        <span className="d-flex justify-content-center mt-4">
                            <ButtonWithSpinner
                                data-testid="term-load-more-btn"
                                color="primary"
                                icon="refresh"
                                isSpinning={loadMore.loading}
                                disabled={loadMore.loading}
                                onClick={() => loadMore.execute(field.name)}
                            >
                                Load more
                            </ButtonWithSpinner>
                        </span>
                    )}
                </AccordionBody>
            </AccordionItem>
        </UncontrolledAccordion>
    );
}

interface IndexTermItemProps {
    indexName: string;
    term: string;
    index: number;
    field: TermsForField;
    fieldTerms: string[];
}

function IndexTermItem({ index, term, indexName, field, fieldTerms }: IndexTermItemProps) {
    const { value: isOpen, toggle: toggleIsOpen } = useBoolean(false);
    const dbName = useAppSelector(databaseSelectors.activeDatabaseName);

    const navigateToQuery = () => {
        const query = queryCriteria.empty();
        const queryText = queryUtil.formatIndexQuery(indexName, field.name, term);

        query.queryText(queryText);
        query.name(`Index terms for ${indexName} (${field.name}: ${term})`);
        query.recentQuery(true);
        const queryDto = query.toStorageDto();
        savedQueriesStorage.saveAndNavigate(dbName, queryDto);
    };

    return (
        <>
            <div data-testid="term-pill" key={index} className="term-pill hstack">
                <div title={term} className="flex-grow-1 text-truncate">
                    {term}
                </div>
                <div className="d-flex gap-1 align-items-center">
                    <Button
                        onClick={() => navigateToQuery()}
                        color="link"
                        size="sm"
                        className="p-0"
                        title="Query index with given term"
                    >
                        <Icon icon="query" margin="m-0" />
                    </Button>
                    <Button onClick={toggleIsOpen} color="link" size="sm" className="p-0" title="Preview item">
                        <Icon icon="preview" margin="m-0" />
                    </Button>
                    <Button
                        onClick={() => copyToClipboard.copy(term, "Index term was copied to clipboard")}
                        color="link"
                        size="sm"
                        className="p-0"
                        title="Copy to clipboard"
                    >
                        <Icon icon="copy-to-clipboard" margin="m-0" />
                    </Button>
                </div>
            </div>
            <IndexTermsPreviewModal
                fieldTerms={fieldTerms}
                termIndex={index}
                isOpen={isOpen}
                toggleModal={toggleIsOpen}
            />
        </>
    );
}
