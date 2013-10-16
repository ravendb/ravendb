package net.ravendb.samples;

import java.util.List;

import com.mysema.query.annotations.QueryEntity;
@QueryEntity
public class Developer {
  private Long id;
  private String nick;
  private Skill mainSkill;
  private List<Skill> skills;

  public Skill getMainSkill() {
    return mainSkill;
  }
  public void setMainSkill(Skill mainSkill) {
    this.mainSkill = mainSkill;
  }
  public Long getId() {
    return id;
  }
  public void setId(Long id) {
    this.id = id;
  }
  public String getNick() {
    return nick;
  }
  public void setNick(String nick) {
    this.nick = nick;
  }
  public List<Skill> getSkills() {
    return skills;
  }
  public void setSkills(List<Skill> skills) {
    this.skills = skills;
  }


}
