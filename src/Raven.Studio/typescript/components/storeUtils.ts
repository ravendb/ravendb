import { addListener, TypedAddListener } from "@reduxjs/toolkit";
import { AppDispatch, RootState } from "components/store";

export const addAppListener = addListener as TypedAddListener<RootState, AppDispatch>;
