package net.ravendb.tests.faceted;

import java.util.Date;
import java.util.List;

import com.mysema.query.annotations.QueryEntity;

@QueryEntity
public class Camera {

  private int id;

  private Date dateOfListing;
  private String manufacturer;
  private String model;
  private Double cost;

  private int zoom;
  private double megapixels;
  private boolean imageStabilizer;
  private List<String> advancedFeatures;

  public int getId() {
    return id;
  }
  public void setId(int id) {
    this.id = id;
  }
  public Date getDateOfListing() {
    return dateOfListing;
  }
  public void setDateOfListing(Date dateOfListing) {
    this.dateOfListing = dateOfListing;
  }
  public String getManufacturer() {
    return manufacturer;
  }
  public void setManufacturer(String manufacturer) {
    this.manufacturer = manufacturer;
  }
  public String getModel() {
    return model;
  }
  public void setModel(String model) {
    this.model = model;
  }
  public Double getCost() {
    return cost;
  }
  public void setCost(Double cost) {
    this.cost = cost;
  }
  public int getZoom() {
    return zoom;
  }
  public void setZoom(int zoom) {
    this.zoom = zoom;
  }
  public double getMegapixels() {
    return megapixels;
  }
  public void setMegapixels(double megapixels) {
    this.megapixels = megapixels;
  }
  public boolean isImageStabilizer() {
    return imageStabilizer;
  }
  public void setImageStabilizer(boolean imageStabilizer) {
    this.imageStabilizer = imageStabilizer;
  }
  public List<String> getAdvancedFeatures() {
    return advancedFeatures;
  }
  public void setAdvancedFeatures(List<String> advancedFeatures) {
    this.advancedFeatures = advancedFeatures;
  }
  @Override
  public String toString() {
    return String.format("%3d: %s %10s - L%4.2f %.1fX zoom, %.1f megapixels, [%s]", id, dateOfListing.toString(), manufacturer,
        model, cost, zoom, megapixels, advancedFeatures == null ? "" :advancedFeatures.toString());
  }
  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((cost == null) ? 0 : cost.hashCode());
    result = prime * result + ((dateOfListing == null) ? 0 : dateOfListing.hashCode());
    result = prime * result + id;
    result = prime * result + (imageStabilizer ? 1231 : 1237);
    result = prime * result + ((manufacturer == null) ? 0 : manufacturer.hashCode());
    long temp;
    temp = Double.doubleToLongBits(megapixels);
    result = prime * result + (int) (temp ^ (temp >>> 32));
    result = prime * result + ((model == null) ? 0 : model.hashCode());
    result = prime * result + zoom;
    return result;
  }
  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    Camera other = (Camera) obj;
    if (cost == null) {
      if (other.cost != null)
        return false;
    } else if (!cost.equals(other.cost))
      return false;
    if (dateOfListing == null) {
      if (other.dateOfListing != null)
        return false;
    } else if (!dateOfListing.equals(other.dateOfListing))
      return false;
    if (id != other.id)
      return false;
    if (imageStabilizer != other.imageStabilizer)
      return false;
    if (manufacturer == null) {
      if (other.manufacturer != null)
        return false;
    } else if (!manufacturer.equals(other.manufacturer))
      return false;
    if (Double.doubleToLongBits(megapixels) != Double.doubleToLongBits(other.megapixels))
      return false;
    if (model == null) {
      if (other.model != null)
        return false;
    } else if (!model.equals(other.model))
      return false;
    if (zoom != other.zoom)
      return false;
    return true;
  }


}
