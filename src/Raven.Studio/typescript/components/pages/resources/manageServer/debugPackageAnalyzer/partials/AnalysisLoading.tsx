import React from "react";
import { LazyLoad } from "components/common/LazyLoad";

export default function AnalysisLoading() {
    return (
        <div className="debug-package-analysis vstack gap-4">
            <div className="d-flex align-items-center justify-content-between gap-4 flex-wrap">
                <LazyLoad active>
                    <div style={{ height: 60, width: 280 }}>&nbsp;</div>
                </LazyLoad>
                <LazyLoad active>
                    <div style={{ height: 32, width: 320 }}>&nbsp;</div>
                </LazyLoad>
            </div>
            <LazyLoad active>
                <div style={{ height: 80 }}>&nbsp;</div>
            </LazyLoad>
            <LazyLoad active>
                <div style={{ height: 160 }}>&nbsp;</div>
            </LazyLoad>
            <div className="d-flex gap-4 flex-wrap">
                <LazyLoad active className="flex-grow-1">
                    <div style={{ height: 120 }}>&nbsp;</div>
                </LazyLoad>
                <LazyLoad active className="flex-grow-1">
                    <div style={{ height: 120 }}>&nbsp;</div>
                </LazyLoad>
                <LazyLoad active className="flex-grow-1">
                    <div style={{ height: 120 }}>&nbsp;</div>
                </LazyLoad>
            </div>
            <LazyLoad active>
                <div style={{ height: 200 }}>&nbsp;</div>
            </LazyLoad>
            <div className="d-flex gap-4 flex-wrap">
                <LazyLoad active className="flex-grow-1">
                    <div style={{ height: 180 }}>&nbsp;</div>
                </LazyLoad>
                <LazyLoad active className="flex-grow-1">
                    <div style={{ height: 180 }}>&nbsp;</div>
                </LazyLoad>
            </div>
        </div>
    );
}
