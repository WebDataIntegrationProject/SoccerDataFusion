package de.uni_mannheim.informatik.dws.wdi.SoccerDataFusion.fusers.club.conflictresolution;

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

public class PlayersUnion<ValueType, RecordType extends Matchable & Fusible<SchemaElementType>, SchemaElementType extends Matchable>
		extends ConflictResolutionFunction<List<ValueType>, RecordType, SchemaElementType> {
	
	private HashMap<String, Player> fusedPlayersMap;
	
	public PlayersUnion(HashMap<String, Player> fusedPlayersMap) {
		super();
		this.fusedPlayersMap = fusedPlayersMap;
	}
	
	@Override
	public FusedValue<List<ValueType>, RecordType, SchemaElementType> resolveConflict(
			Collection<FusibleValue<List<ValueType>, RecordType, SchemaElementType>> values) {

		HashSet<ValueType> union = new HashSet<>();

		for (FusibleValue<List<ValueType>, RecordType, SchemaElementType> value : values) {
			List<Player> players = (List<Player>) value.getValue();
			List<Player> fusedPlayers = new LinkedList<Player>();
			for (Player player : players) {
				Player fusedPlayer = this.fusedPlayersMap.get(player.getIdentifier());
				if (fusedPlayer == null) {
					fusedPlayers.add(player);
				} else if (fusedPlayer.getClubMembershipValidAsOf().equals(player.getClubMembershipValidAsOf())) {
					fusedPlayers.add(fusedPlayer);
				} else {
					// Do nothing (ensures that players are unique according to the correspondenses 
				}
			}
			union.addAll((List<ValueType>) fusedPlayers);
		}
		List<ValueType> list = new LinkedList<>(union);
		FusedValue<List<ValueType>, RecordType, SchemaElementType> fused = new FusedValue<>(list);

		for (FusibleValue<List<ValueType>, RecordType, SchemaElementType> value : values) {
			fused.addOriginalRecord(value);
		}

		return fused;
	}
}
