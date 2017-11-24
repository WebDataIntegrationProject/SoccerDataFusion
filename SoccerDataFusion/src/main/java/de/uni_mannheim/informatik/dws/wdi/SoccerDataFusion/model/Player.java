/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import de.uni_mannheim.informatik.dws.winter.model.AbstractRecord;
import de.uni_mannheim.informatik.dws.winter.model.defaultmodel.Attribute;

/**
 * A {@link AbstractRecord} which represents an actor
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class Player extends AbstractRecord<Attribute> implements Serializable {

	/*
	 <player>
       <fullName>Manuel Neuer</fullName>
     	<!-- we use the following format for dates: yyyy-mm-dd -->
     	<birthDate>1986-03-27</birthDate>
       <!-- full name of birthplace in English-->
     	<birthplace>Gelsenkirchen</birthplace>
     	<!-- ISO 3166 alpha 3 country code -->
     	<nationality>DEU</nationality>
       <!-- height in cm. Integers, no floating point number. -->
       <height>193</height>
       <!-- weight in kg. Integers, no floating point number.  -->
       <weight>92</weight>
       <!-- shirt number as two character string -->
     	<shirtNumberOfClub>01</shirtNumberOfClub>
     	<shirtNumberOfNationalTeam>01</shirtNumberOfNationalTeam>
       <!-- two character string. We use the following positions: GK (goal keeper), DF (defense), MF (middle field), FW (forward) -->
     	<position>GK</position>
       <preferredFoot>right</preferredFoot>
     	<caps>64</caps>
       <isInNationalTeam>TRUE</isInNationalTeam>
       <!-- we use the following format for dates: yyyy-mm-dd -->
     	<clubMembershipValidAsOf>2017-10-12</clubMembershipValidAsOf>
     </player>
	 */

	private static final long serialVersionUID = 1L;
	private String fullName;
	private LocalDateTime birthDate;
	private String birthplace;
	private String nationality;
	private Integer height;
	private Integer weight;
	private String shirtNumberOfClub;
	private String shirtNumberOfNationalTeam;
	private String position;
	private String preferredFoot;
	private Integer caps;
	private Boolean isInNationalTeam;
	private LocalDateTime clubMembershipValidAsOf;
	private String clubName;

	public Player(String identifier, String provenance) {
		super(identifier, provenance);
	}
	
	public String getClubName() {return clubName; }

	public void setClubName(String clubName) { this.clubName = clubName; }

	public String getFullName() {
		return fullName;
	}

	public void setFullName(String fullName) {
		this.fullName = fullName;
	}

	public LocalDateTime getBirthDate() {
		return birthDate;
	}

	public void setBirthday(LocalDateTime birthDate) {
		this.birthDate = birthDate;
	}

	public String getBirthplace() {
		return birthplace;
	}

	public void setBirthplace(String birthplace) {
		this.birthplace = birthplace;
	}

	public String getNationality() {
		return nationality;
	}

	public void setNationality(String nationality) {
		this.nationality = nationality;
	}

	public Integer getHeight() {
		return height;
	}

	public void setHeight(Integer height) {
		this.height = height;
	}

	public Integer getWeight() {
		return weight;
	}

	public void setWeight(Integer weight) {
		this.weight = weight;
	}
	public String getShirtNumberOfClub() {
		return shirtNumberOfClub;
	}

	public void setShirtNumberOfClub(String shirtNumberOfClub) {
		this.shirtNumberOfClub = shirtNumberOfClub;
	}

	public String getShirtNumberOfNationalTeam() {
		return shirtNumberOfNationalTeam;
	}

	public void setShirtNumberOfNationalTeam(String shirtNumberOfNationalTeam) {
		this.shirtNumberOfNationalTeam = shirtNumberOfNationalTeam;
	}

	public String getPosition() {
		return position;
	}

	public void setPosition(String position) {
		this.position = position;
	}

	public String getPreferredFoot() {
		return preferredFoot;
	}

	public void setPreferredFoot(String preferredFoot) {
		this.preferredFoot = preferredFoot;
	}

	public Integer getCaps() {
		return caps;
	}

	public void setCaps(Integer caps) {
		this.caps = caps;
	}

	public Boolean getIsInNationalTeam() {
		return isInNationalTeam;
	}

	public void setIsInNationalTeam(Boolean isInNationalTeam) {
		this.isInNationalTeam = isInNationalTeam;
	}

	public LocalDateTime getClubMembershipValidAsOf() {
		return clubMembershipValidAsOf;
	}

	public void setClubMembershipValidAsOf(LocalDateTime clubMembershipValidAsOf) {
		this.clubMembershipValidAsOf = clubMembershipValidAsOf;
	}


	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		int result = 31 + ((fullName == null) ? 0 : fullName.hashCode());
		return result;
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Player other = (Player) obj;
		if (fullName == null) {
			if (other.fullName != null)
				return false;
		} else if (!fullName.equals(other.fullName))
			return false;
		return true;
	}
	
	private Map<Attribute, Collection<String>> provenance = new HashMap<>();
	private Collection<String> recordProvenance;

	public void setRecordProvenance(Collection<String> provenance) {
		recordProvenance = provenance;
	}

	public Collection<String> getRecordProvenance() {
		return recordProvenance;
	}

	public void setAttributeProvenance(Attribute attribute,
			Collection<String> provenance) {
		this.provenance.put(attribute, provenance);
	}

	public Collection<String> getAttributeProvenance(String attribute) {
		return provenance.get(attribute);
	}

	public String getMergedAttributeProvenance(Attribute attribute) {
		Collection<String> prov = provenance.get(attribute);

		if (prov != null) {
			return StringUtils.join(prov, "+");
		} else {
			return "";
		}
	}

	public static final Attribute FULLNAME = new Attribute("Name");
	public static final Attribute BIRTHPLACE = new Attribute("Birthplace");
	public static final Attribute BIRTHDATE = new Attribute("Birthdate");
	public static final Attribute NATIONALITY = new Attribute("Nationality");
	public static final Attribute HEIGHT = new Attribute("Height");
	public static final Attribute WEIGHT = new Attribute("Weight");	
	public static final Attribute SHIRTNUMBEROFCLUB = new Attribute("ShirtNumberOfClub");
	public static final Attribute SHIRTNUMBEROFNATIONALTEAM = new Attribute("ShirtNumberOfNationalTeam");
	public static final Attribute POSITION = new Attribute("Position");
	public static final Attribute PREFERREDFOOT = new Attribute("PreferredFoot");
	public static final Attribute CAPS = new Attribute("Caps");
	public static final Attribute ISINNATIONALTEAM = new Attribute("IsInNationalTeam");
	public static final Attribute CLUBMEMBERSHIPVALIDASOF = new Attribute("ClubMembershipValidAsOf");
	public static final Attribute CLUBNAME = new Attribute("ClubName");
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Record#hasValue(java.lang.Object)
	 */
	@Override
	public boolean hasValue(Attribute attribute) {
		if(attribute==FULLNAME)
			return fullName!=null;
		else if(attribute==BIRTHPLACE) 
			return birthplace!=null;
		else if(attribute==BIRTHDATE)
			return birthDate!=null;
		else if(attribute==NATIONALITY)
			return nationality!=null;
		else if(attribute==HEIGHT)
			return height!=null;
		else if(attribute==WEIGHT)
			return weight!=null;
		else if(attribute==SHIRTNUMBEROFCLUB)
			return shirtNumberOfClub!=null;
		else if(attribute==SHIRTNUMBEROFNATIONALTEAM)
			return shirtNumberOfNationalTeam!=null;
		else if(attribute==POSITION)
			return position!=null;
		else if(attribute==PREFERREDFOOT)
			return preferredFoot!=null;
		else if(attribute==CAPS)
			return caps!=null;
		else if(attribute==ISINNATIONALTEAM)
			return isInNationalTeam!=null;
		else if(attribute==CLUBMEMBERSHIPVALIDASOF)
			return clubMembershipValidAsOf!=null;
		else if(attribute==CLUBNAME)
			return clubName!=null;
		return false;
	}

}
