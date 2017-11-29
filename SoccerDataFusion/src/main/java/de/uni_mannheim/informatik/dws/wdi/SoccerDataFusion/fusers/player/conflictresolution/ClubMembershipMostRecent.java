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

public class ClubMembershipMostRecent<ValueType, RecordType extends Matchable & Fusible<SchemaElementType>, SchemaElementType extends Matchable>
		extends ConflictResolutionFunction<ValueType, RecordType, SchemaElementType> {
	
	public ClubMembershipMostRecent() {
		super();
	}
	
	@Override
	public FusedValue<ValueType, RecordType, SchemaElementType> resolveConflict(
			Collection<FusibleValue<ValueType, RecordType, SchemaElementType>> values) {

		LocalDateTime mostRecent = null;

		for (FusibleValue<ValueType, RecordType, SchemaElementType> value : values) {
			LocalDateTime clubMembership = (LocalDateTime) value.getValue();
			if (mostRecent == null || clubMembership.isAfter(mostRecent)){
				mostRecent = clubMembership;
			}
		}
		ValueType mostRecentDate = (ValueType) mostRecent;
		FusedValue<ValueType, RecordType, SchemaElementType> fused = new FusedValue<>(mostRecentDate);

		for (FusibleValue<ValueType, RecordType, SchemaElementType> value : values) {
			fused.addOriginalRecord(value);
		}

		return fused;
	}
}
