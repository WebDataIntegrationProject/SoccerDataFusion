package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.player.conflictresolution;

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

public class PlayerMostRecent<ValueType, RecordType extends Matchable & Fusible<SchemaElementType>, SchemaElementType extends Matchable> extends
		ConflictResolutionFunction<ValueType, RecordType, SchemaElementType> {
	
	@Override
	public FusedValue<ValueType, RecordType, SchemaElementType> resolveConflict(
			Collection<FusibleValue<ValueType, RecordType, SchemaElementType>> values) {
		
		FusibleValue<ValueType, RecordType, SchemaElementType> mostRecentValue = null;
		Player mostRecentPlayer = null;
		
		for(FusibleValue<ValueType, RecordType, SchemaElementType> value : values) {
			Player player = (Player) value.getRecord();
			if(mostRecentValue==null || player.getClubMembershipValidAsOf().isAfter(mostRecentPlayer.getClubMembershipValidAsOf())) {
				mostRecentValue = value;
				mostRecentPlayer = player;
			}
		}
		
		return new FusedValue<>(mostRecentValue);
	}
}
