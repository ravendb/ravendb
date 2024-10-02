import React from "react";
import RichAlert from "components/common/RichAlert";

export interface IndexInfoForDelete {
    indexName: string;
    reduceOutputCollection: string;
    referenceCollection: string;
}

export interface DeleteIndexesConfirmBodyProps {
    lockedIndexNames: string[];
    indexesInfoForDelete: IndexInfoForDelete[];
}

export default function DeleteIndexesConfirmBody({
    lockedIndexNames,
    indexesInfoForDelete,
}: DeleteIndexesConfirmBodyProps) {
    const showWarning = indexesInfoForDelete.some((x) => x.reduceOutputCollection);

    return (
        <div>
            {indexesInfoForDelete.length > 0 && (
                <div>
                    <p>
                        You are deleting{" "}
                        {indexesInfoForDelete.length > 1 ? (
                            <span>
                                <strong>{indexesInfoForDelete.length}</strong> indexes:
                            </span>
                        ) : (
                            <span>index:</span>
                        )}
                    </p>
                    <ul className="overflow-auto" style={{ maxHeight: "200px" }}>
                        {indexesInfoForDelete.map((x) => (
                            <li key={x.indexName}>
                                <strong>{x.indexName}</strong>
                                {x.reduceOutputCollection && (
                                    <small className="ms-1">
                                        {"("}Reduce Results Collections: {x.reduceOutputCollection}
                                        {x.referenceCollection && `, ${x.referenceCollection}`}
                                        {")"}
                                    </small>
                                )}
                            </li>
                        ))}
                    </ul>
                </div>
            )}
            {lockedIndexNames.length > 0 && (
                <div>
                    <p>Skipping locked indexes:</p>
                    <ul className="overflow-auto" style={{ maxHeight: "200px" }}>
                        {lockedIndexNames.map((x) => (
                            <li key={x}>
                                <strong>{x}</strong>
                            </li>
                        ))}
                    </ul>
                </div>
            )}
            {showWarning && (
                <>
                    <hr />
                    <RichAlert variant="warning">
                        Note: <br />
                        &apos;Reduce Results Collections&apos; were created by above index(es).
                        <br />
                        Clicking &apos;Delete&apos; will delete the index(es) but Not the Results Collection(s).
                        <br />
                        Go to the collection itself to manually remove documents.
                    </RichAlert>
                </>
            )}
        </div>
    );
}
