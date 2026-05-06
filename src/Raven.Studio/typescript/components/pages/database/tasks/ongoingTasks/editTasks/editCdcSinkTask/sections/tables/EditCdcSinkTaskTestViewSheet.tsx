// import ButtonWithSpinner from "components/common/ButtonWithSpinner";
// import Code from "components/common/Code";
// import { EmptySet } from "components/common/EmptySet";
// import { FormGroup, FormLabel } from "components/common/Form";
// import { ViewSheet } from "components/common/splitView/ViewSheet";
// import {
//     editCdcSinkTaskActions,
//     editCdcSinkTaskSelectors,
//     testCdcSinkTransformation,
// } from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
// import { useAppDispatch, useAppSelector } from "components/store";
// import Form from "react-bootstrap/Form";
// import { TableNode } from "./editCdcSinkTaskTableHelpers";

// type CdcSinkConfiguration = Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration;

// interface EditCdcSinkTaskTestViewSheetProps {
//     selectedNode: TableNode;
//     databaseName: string;
//     connectionStringName: string;
//     getConfiguration: () => CdcSinkConfiguration;
// }

// export default function EditCdcSinkTaskTestViewSheet(props: EditCdcSinkTaskTestViewSheetProps) {
//     const { selectedNode, databaseName, connectionStringName, getConfiguration } = props;
//     const dispatch = useAppDispatch();
//     const testMessage = useAppSelector(editCdcSinkTaskSelectors.testMessage);
//     const sourceRecordId = useAppSelector(editCdcSinkTaskSelectors.sourceRecordId);
//     const testResult = useAppSelector(editCdcSinkTaskSelectors.testResult);

//     const handleRunTest = async () => {
//         if (!testMessage.trim()) {
//             dispatch(
//                 editCdcSinkTaskActions.testResultFailed(
//                     "Provide a sample CDC message payload before running the transformation preview."
//                 )
//             );
//             return;
//         }

//         await dispatch(
//             testCdcSinkTransformation({
//                 databaseName,
//                 dto: {
//                     Configuration: getConfiguration(),
//                     Message: testMessage,
//                 },
//             })
//         );
//     };

//     return (
//         <ViewSheet className="h-100">
//             <ViewSheet.Header>
//                 <div>
//                     <div className="fw-semibold">Test Single Record</div>
//                     <div className="small text-muted">{selectedNode.subtitle}</div>
//                 </div>
//             </ViewSheet.Header>
//             <ViewSheet.Body>
//                 <div className="d-flex flex-column gap-3 h-100">
//                     <FormGroup marginClass="mb-0">
//                         <FormLabel>Source Record ID</FormLabel>
//                         <Form.Control
//                             type="text"
//                             value={sourceRecordId}
//                             placeholder="Placeholder for future record lookup"
//                             onChange={(event) =>
//                                 dispatch(editCdcSinkTaskActions.sourceRecordIdChanged(event.target.value))
//                             }
//                         />
//                         <div className="small text-muted mt-2">
//                             Keep this field as a placeholder until the ID-based fetch endpoint is available.
//                         </div>
//                     </FormGroup>

//                     <FormGroup marginClass="mb-0">
//                         <FormLabel>Sample CDC Message</FormLabel>
//                         <Form.Control
//                             as="textarea"
//                             rows={8}
//                             value={testMessage}
//                             placeholder="Paste a CDC event payload here"
//                             onChange={(event) =>
//                                 dispatch(editCdcSinkTaskActions.testMessageChanged(event.target.value))
//                             }
//                         />
//                     </FormGroup>

//                     <div className="d-flex align-items-center gap-3">
//                         <ButtonWithSpinner
//                             isSpinning={testResult.status === "loading"}
//                             variant="primary"
//                             onClick={handleRunTest}
//                             disabled={!connectionStringName}
//                         >
//                             Test
//                         </ButtonWithSpinner>
//                         {!connectionStringName && (
//                             <div className="small text-muted">Select a connection string first.</div>
//                         )}
//                     </div>

//                     {testResult.status === "failure" && <div className="small text-danger">{testResult.error}</div>}

//                     <div className="d-flex flex-column gap-3 flex-grow-1">
//                         <div className="border rounded overflow-hidden">
//                             <div className="px-3 py-2 border-bottom fw-semibold">Transformed Actions</div>
//                             {testResult.status === "success" ? (
//                                 <Code
//                                     code={JSON.stringify(testResult.data?.Actions ?? {}, null, 4)}
//                                     language="json"
//                                     className="mb-0"
//                                 />
//                             ) : (
//                                 <EmptySet compact className="py-4">
//                                     Run the preview to inspect the generated document actions.
//                                 </EmptySet>
//                             )}
//                         </div>

//                         <div className="border rounded overflow-hidden flex-grow-1">
//                             <div className="px-3 py-2 border-bottom fw-semibold">Debug Output</div>
//                             {testResult.status === "success" && testResult.data?.DebugOutput?.length ? (
//                                 <Code
//                                     code={testResult.data.DebugOutput.join("\n")}
//                                     language="plaintext"
//                                     className="mb-0"
//                                 />
//                             ) : (
//                                 <EmptySet compact className="py-4">
//                                     No debug output yet.
//                                 </EmptySet>
//                             )}
//                         </div>
//                     </div>
//                 </div>
//             </ViewSheet.Body>
//         </ViewSheet>
//     );
// }
