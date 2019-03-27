def lig_pos_to_occupancy_refinement_string(lig_pos):
    """
    Write occupancy refinement parameters for refmac single model

    Parameters
    ----------
    lig_pos

    Returns
    -------

    """
    group_lines = []
    incomplete_lines = []

    # Loop over ligand information
    for occ_group, pos in enumerate(lig_pos):

        # Get chain and residue number
        chain = pos[0]
        resid = pos[2]

        # Format lines
        # Occupancy group is seperate for each residue,
        # and comes from the iterable
        group_line = "occupancy group id {} chain {} resi {}".format(occ_group+1, chain, resid)
        incomplete_line = "occupancy group alts incomplete {}".format(occ_group+1)

        # Append lines to lists
        group_lines.append(group_line)
        incomplete_lines.append(incomplete_line)

    # Write saved lines as single string
    refinement_str = '\n'.join(group_lines) +'\n' + \
                     '\n'.join(incomplete_lines) + '\n' + \
                     'occupancy refine' + '\n'

    return refinement_str