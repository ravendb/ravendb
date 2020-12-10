import viewModelBase = require("viewmodels/viewModelBase");
import spatialMapLayerModel = require("models/database/query/spatialMapLayerModel");
import document = require("models/database/documents/document");
import documentMetadata = require("models/database/documents/documentMetadata");
import {Control, Layer, MarkerClusterGroup} from "leaflet";
import genUtils = require("common/generalUtils");

class spatialQueryMap extends viewModelBase {
    
    geoData = ko.observableArray<spatialMapLayerModel>([]);

    constructor(geoData: Array<spatialMapLayerModel>) {
        super();
        this.geoData(geoData);
    }

    compositionComplete() {
        super.compositionComplete();
        this.createMap();
    }
    
    private createMap() {

        const osmMap = this.getStreetMapTileLayer();
        const otmMap = this.getTopographyMapTileLayer();
        const baseLayers =  {
            "Streets Map": osmMap,
            "Topography Map": otmMap
        };

        const generatePopupContent = (doc: document) => {
            const docDto = doc.toDto(true);
            const metaDto = docDto["@metadata"];
            documentMetadata.filterMetadata(metaDto);

            let text = JSON.stringify(docDto, null, 4);
            text = Prism.highlight(text, (Prism.languages as any)["javascript"]);

            const textHtml = `<div>
                                    <h4>Document: ${genUtils.escapeHtml(doc.getId())}</h4>
                                    <hr>
                                    <pre>${text}</pre>
                              </div>`;
            return textHtml;
        }
        
        const dataLayers: Control.LayersObject = {};
        let markersGroups: MarkerClusterGroup[] = [];

        this.geoData().forEach((layerModel) => {
            let markers: Layer[] = [];
            let markersGroup = L.markerClusterGroup();

            layerModel.geoPoints().forEach((point) => {
                const pointMarker = L.marker([point.latitude, point.longitude], { title: point.popupContent.getId() })
                                     .bindPopup(generatePopupContent(point.popupContent), { 'className' : 'custom-popup', 'maxWidth': 600, 'maxHeight': 400 } );
                markers.push(pointMarker);
            });

            markersGroup.addLayers(markers);
            const numberOfMarkersInLayer = markers.length;
            
            const controlLayerText = `<span>Point fields: <span class="number-of-markers pull-right padding padding-xs">${numberOfMarkersInLayer}</span></span>
                                      <div class='margin-left'>
                                          <span>${genUtils.escapeHtml(layerModel.latitudeProperty)}</span><br>
                                          <span>${genUtils.escapeHtml(layerModel.longitudeProperty)}</span>
                                      </div>`;

            // A markersGroup layer is per spatial point from query
            (dataLayers as any)[controlLayerText] = markersGroup;
            markersGroups.push(markersGroup);
        })

        const map = L.map("mapid", {preferCanvas: true});
        
        osmMap.addTo(map);
        L.control.layers(baseLayers, dataLayers).addTo(map);
        markersGroups.forEach(group => map.addLayer(group));
  
        const mapBounds = L.latLngBounds([]);
        markersGroups.forEach(group => mapBounds.extend(group.getBounds()));
        map.fitBounds(mapBounds, {padding: [50, 50]});
    }
    
    private getStreetMapTileLayer() {
        const osmLink = `<a href="http://openstreetmap.org">OpenStreetMap</a>`;
        const osmUrl = 'http://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png';
        const osmAttrib = `&copy; ${osmLink} Contributors`;
        return L.tileLayer(osmUrl, {attribution: osmAttrib});
    }
    
    private getTopographyMapTileLayer() {
        const otmLink = `<a href="http://opentopomap.org/">OpenTopoMap</a>`;
        const otmUrl = `http://{s}.tile.opentopomap.org/{z}/{x}/{y}.png`;
        const otmAttrib = `&copy; ${otmLink} Contributors`;
        return L.tileLayer(otmUrl, {attribution: otmAttrib});
    } 
}

export = spatialQueryMap;
