package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.conflictresolution;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import de.uni_mannheim.informatik.dws.winter.datafusion.conflictresolution.ConflictResolutionFunction;
import de.uni_mannheim.informatik.dws.winter.model.FusedValue;
import de.uni_mannheim.informatik.dws.winter.model.Fusible;
import de.uni_mannheim.informatik.dws.winter.model.FusibleValue;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.model.Player;

public class IsInNationalTeamMostRecent<ValueType, RecordType extends Matchable & Fusible<SchemaElementType>, SchemaElementType extends Matchable>
		extends ConflictResolutionFunction<ValueType, RecordType, SchemaElementType> {
	
	public IsInNationalTeamMostRecent() {
		super();
	}
	
	@Override
	public FusedValue<ValueType, RecordType, SchemaElementType> resolveConflict(
			Collection<FusibleValue<ValueType, RecordType, SchemaElementType>> values) {

		LocalDateTime mostRecentDate = null;
		Boolean inNationalTeam = false;
		
		for (FusibleValue<ValueType, RecordType, SchemaElementType> value : values) {
			if ((Boolean) value.getValue()){
				inNationalTeam = true;
				break;
			}
//			Player player = (Player) value.getRecord();
//			LocalDateTime clubMembership = player.getClubMembershipValidAsOf();
//			if (mostRecentDate == null || clubMembership.isAfter(mostRecentDate)){
//				mostRecentDate = clubMembership;
//				inNationalTeam = (Boolean) value.getValue();
//			}
		}
		ValueType vtInNationalTeam = (ValueType) inNationalTeam;
		FusedValue<ValueType, RecordType, SchemaElementType> fused = new FusedValue<>(vtInNationalTeam);

		for (FusibleValue<ValueType, RecordType, SchemaElementType> value : values) {
			fused.addOriginalRecord(value);
		}

		return fused;
	}
}
