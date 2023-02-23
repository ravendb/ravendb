// This component is only displayed in the storybook. To modify the actual splash screen, make changes in wwwroot/index.html

import React from "react";
import "./SplashScreen.scss";

export function SplashScreen() {
    return (
        <div className="splash-screen">
            <div className="spinner-box">
                <div className="orbit orbit-1">
                    <i className="icon-raven fs-1 text-white"></i>
                </div>

                <div className="orbit orbit-2">
                    <i className="orbit-icon"></i>
                </div>
                <div className="orbit orbit-3">
                    <i className="orbit-icon"></i>
                </div>
            </div>
            <p className="lead text-center">Building a nest</p>
        </div>
    );
}
