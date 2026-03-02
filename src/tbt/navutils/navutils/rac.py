"""Functions for the Route Agreement Checker"""

import logging
import warnings

import numpy
import shapely.geometry
import shapely.ops
import shapely.wkt

from .provider import Provider
from .route import DistanceOnEarth

log = logging.getLogger(__name__)

# Geometry buffer in degrees to assume that two geometries are the same
BUFFER = 0.0002
# Stopping criterion for splitting a critical section (length in meters)
CRITICAL_SECTION_LOWER_IN_METERS = 100
# Stopping criterion for splitting a critical section (length in degrees)
CRITICAL_SECTION_LOWER_IN_DEGREES = 5 * BUFFER
# Distance (in degrees) that we add to the start and end of a critical section to make sure
# that we are including the error in the critical section
BACKWARD_AND_FORWARD_STEP_IN_DEGREES = 2 * BUFFER
# Maximum number of deep iterations (max size of the binary tree)
# when splitting critical sections
MAX_ITERATIONS = 5
# Maximum number of competitor api calls allowed before we throw an error and stop
MAX_API_CALLS = 1000
# Number of competitor api calls before thowing a warning
MAX_API_CALLS_WARN = 50


def interval_union_sections(sections):
    result = []
    for begin, end in sorted(sections):
        if result and result[-1][1] >= begin:
            result[-1][1] = max(result[-1][1], end)
        else:
            result.append([begin, end])
    return result


def extract_sections(diff, baseline_section, proj_start, proj_end):
    sections = []
    if isinstance(diff, shapely.geometry.LineString) and len(diff.coords) >= 2:
        diff = shapely.geometry.MultiLineString([diff])

    if isinstance(diff, shapely.geometry.MultiLineString):
        for geom in diff.geoms:
            if isinstance(geom, shapely.geometry.LineString):
                start, end = sorted(
                    (
                        baseline_section.project(
                            shapely.geometry.Point(geom.coords[0])
                        ),
                        baseline_section.project(
                            shapely.geometry.Point(geom.coords[-1])
                        ),
                    )
                )
                start = min(
                    max(
                        start - BACKWARD_AND_FORWARD_STEP_IN_DEGREES,
                        proj_start,
                    ),
                    proj_end,
                )
                end = max(
                    min(end + BACKWARD_AND_FORWARD_STEP_IN_DEGREES, proj_end),
                    proj_start,
                )
                if start < end:
                    sections.append((start, end))
            else:
                raise TypeError("unknown geometry type")
    return sections


def get_intervals(baseline_section, competitor_section, section, proj_start, proj_end):
    sections = []

    diff1 = section.difference(competitor_section.buffer(BUFFER))
    sections += extract_sections(diff1, baseline_section, proj_start, proj_end)

    diff2 = competitor_section.difference(section.buffer(BUFFER))
    sections += extract_sections(diff2, baseline_section, proj_start, proj_end)

    sections = interval_union_sections(sections)
    return sections


class CriticalSectionIteration:
    """RAC iterative funcion
    Stopping criteria:

    1. section equal to competitor's route (potential_error = False)
    2. section length < CRITICAL_SECTION_LOWER_IN_METERS (potential_error = True)
    3. section length < CRITICAL_SECTION_LOWER_IN_DEGREES (potential_error = True)
    4. reached more than MAX_ITERATIONS deep iterations
    5. the splitted interval to iterate are bigger than the current critical section
    6. iteration > 0 (subsplit) and missalignment > BUFFER at start or
     end of critical section and section length < CRITICAL_SECTION_LOWER_IN_METERS*10
    """

    def __init__(
        self,
        baseline_section: shapely.geometry.LineString,
        competitor: Provider,
        proj_start=None,
        proj_end=None,
        iteration=0,
        competitor_section=None,
    ) -> None:
        if proj_start is None or proj_end is None:
            self.proj_start = 0
            self.proj_end = baseline_section.length
        else:
            self.proj_start = proj_start
            self.proj_end = proj_end

        self.baseline_section = baseline_section
        self.section = shapely.ops.substring(
            self.baseline_section, self.proj_start, self.proj_end
        )
        self.competitor = competitor
        self.critical_sections = []
        self.children = []
        self.competitor_calls = 0

        # compute length in meters of section
        self.section_length = numpy.sum(
            [
                DistanceOnEarth.get_distance_from_lat_lon_in_m(
                    lat1=self.section.coords[i - 1][1],
                    lon1=self.section.coords[i - 1][0],
                    lat2=self.section.coords[i][1],
                    lon2=self.section.coords[i][0],
                )
                for i in range(1, len(self.section.coords))
            ]
        )
        if iteration > MAX_ITERATIONS:
            # Stop, we have iterated too many times,
            #  generating potentially 2^MAX_ITERATIONS api calls...
            self.potential_error = True
            self.critical_sections = [(self.proj_start, self.proj_end)]
            warnings.warn(
                f"Reached {MAX_ITERATIONS} deep iterations in CriticalSectionIteration"
            )
        else:
            # Compute competitor route
            if competitor_section is None:
                self.competitor_section = competitor.getRoute(
                    self.section.coords[0], self.section.coords[-1]
                ).geometry
                self.competitor_calls += 1
            else:
                # We saved an api call :)
                self.competitor_section = competitor_section

            # Simplify geometries so we do not have problems with the buffers
            self.competitor_section = self.competitor_section.simplify(tolerance=0.0001)
            self.section = self.section.simplify(tolerance=0.0001)

            # Check if baseline and provider route are 'equal' up to a buffer
            if self.competitor_section.buffer(BUFFER).contains(
                self.section
            ) and self.section.buffer(BUFFER).contains(self.competitor_section):
                self.potential_error = False
            else:
                # There is a potential deviation
                if (
                    self.section_length < CRITICAL_SECTION_LOWER_IN_METERS
                    or self.section.length < CRITICAL_SECTION_LOWER_IN_DEGREES
                ):
                    # If we reached the minimum length, then we should not continue iterating
                    self.potential_error = True
                    self.critical_sections = [[self.proj_start, self.proj_end]]

                elif (
                    (self.competitor_section.length > 0)
                    and (iteration > 0)
                    and (self.section_length < CRITICAL_SECTION_LOWER_IN_METERS * 10)
                    and (
                        (
                            self.section.interpolate(0).distance(
                                self.competitor_section.interpolate(0)
                            )
                            > BUFFER
                        )
                        or (
                            self.section.interpolate(self.section.length).distance(
                                self.competitor_section.interpolate(
                                    self.competitor_section.length
                                )
                            )
                            > BUFFER
                        )
                    )
                ):
                    # There is missalignment at the start or end of this section,
                    #  it is not worth it splitting, since we will get many errors.
                    self.potential_error = True
                    self.critical_sections = [[self.proj_start, self.proj_end]]
                    warnings.warn(
                        f"Stopping since section_length = {self.section_length}"
                        f" and iteration = {iteration} and missalignment at the start/end."
                        f" baseline_section='{self.baseline_section.wkt}'"
                    )
                else:
                    # If we still have a long section, continue
                    self.potential_error = False
                    intervals = get_intervals(
                        self.baseline_section,
                        self.competitor_section,
                        self.section,
                        self.proj_start,
                        self.proj_end,
                    )
                    for section in intervals:
                        # split it in half and create two children CriticalSectionIteration

                        if (
                            section[0] <= self.proj_start
                            and (section[0] + section[1]) / 2 + BUFFER >= self.proj_end
                        ):
                            # If we continued iterating we would be expanding (and not decreasing)
                            #  the section size, so we must stop!
                            self.potential_error = True
                            self.critical_sections += [[self.proj_start, self.proj_end]]
                        else:
                            iteration1 = CriticalSectionIteration(
                                self.baseline_section,
                                self.competitor,
                                section[0],
                                (section[0] + section[1]) / 2 + BUFFER,
                                iteration + 1,
                            )

                            self.competitor_calls += iteration1.competitor_calls
                            if iteration1.potential_error:
                                self.potential_error = True
                                self.critical_sections += iteration1.critical_sections
                            self.children.append(iteration1)

                        if (
                            section[0] + section[1]
                        ) - BUFFER <= self.proj_start and section[1] >= self.proj_end:
                            # If we continued iterating we would be expanding (and not decreasing)
                            #  the section size, so we must stop!
                            self.potential_error = True
                            self.critical_sections += [[self.proj_start, self.proj_end]]
                        else:
                            iteration2 = CriticalSectionIteration(
                                self.baseline_section,
                                self.competitor,
                                (section[0] + section[1]) / 2 - BUFFER,
                                section[1],
                                iteration + 1,
                            )
                            self.competitor_calls += iteration2.competitor_calls
                            if iteration2.potential_error:
                                self.critical_sections += iteration2.critical_sections
                                self.potential_error = True
                            self.children.append(iteration2)

            self.critical_sections = interval_union_sections(self.critical_sections)
            self.critical_sections_geometry = [
                shapely.ops.substring(baseline_section, x[0], x[1])
                for x in self.critical_sections
            ]
            if self.competitor_calls > MAX_API_CALLS:
                raise RecursionError(
                    f"Violation of limit of competitor API calls."
                    f" If this is normal, increase MAX_API_CALLS={MAX_API_CALLS} in rac.py,"
                    f" baseline_section='{self.baseline_section.wkt}'"
                )
            if self.competitor_calls > MAX_API_CALLS_WARN:
                warnings.warn(
                    f"Api calls to {competitor.name} exceeded {MAX_API_CALLS_WARN}"
                    f" ({self.competitor_calls})."
                    f" If this is normal, increase MAX_API_CALLS_WARN in rac.py,"
                    f" baseline_section='{self.baseline_section.wkt}'"
                )
