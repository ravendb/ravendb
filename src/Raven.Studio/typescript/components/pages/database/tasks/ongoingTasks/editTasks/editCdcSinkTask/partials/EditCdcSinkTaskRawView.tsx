import AceEditor from "components/common/ace/AceEditor";
import {
    editCdcSinkTaskActions,
    editCdcSinkTaskSelectors,
} from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/store/editCdcSinkTaskSlice";
import { useAppDispatch, useAppSelector } from "components/store";

interface EditCdcSinkTaskRawViewProps {
    heightPx: number;
}

export default function EditCdcSinkTaskRawView({ heightPx }: EditCdcSinkTaskRawViewProps) {
    const dispatch = useAppDispatch();
    const rawViewContent = useAppSelector(editCdcSinkTaskSelectors.rawViewContent);
    return (
        <AceEditor
            mode="json"
            value={rawViewContent}
            onChange={(x) => dispatch(editCdcSinkTaskActions.rawViewContentSet(x))}
            height={`${heightPx}px`}
            maxHeight={Infinity}
        />
    );
}
